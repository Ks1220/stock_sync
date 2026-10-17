from concurrent.futures import ThreadPoolExecutor, as_completed

import frappe
import requests

# /assetStock returns ~9800 items (~30MB); /stockitem/{docKey} must be called
# once per item to get its qty. The origin API breaks down above ~5 concurrent
# connections, so keep this low and write in chunks so progress isn't lost.
QTY_FETCH_WORKERS = 4
RECONCILE_CHUNK_SIZE = 200

STOCK_GROUP_MAPPINGS = [
    {"code": "AC", "description": "AIR COMPRESSOR"},
    {"code": "DEFAULT", "description": "DEFAULT"},
    {"code": "FB", "description": "FABRICATION OF SLING"},
    {"code": "GENSET", "description": "GENSET"},
    {"code": "HF", "description": "HYDRAULIC HOSE & FITTING"},
    {"code": "HK", "description": "Hook"},
    {"code": "HYD", "description": "Hydraulic Hose Fitting"},
    {"code": "ISP", "description": "INSPECTION"},
    {"code": "L99", "description": "LOT 99 DEVELOPMENT INVENTORY"},
]
ALLOWED_PREFIXES = [group["code"] for group in STOCK_GROUP_MAPPINGS]


def _get_settings():
    conf = frappe.conf
    return {
        "api_base": conf.get("stock_sync_api_base", "https://api.qtechline.com"),
        "client_id": conf.get("stock_sync_cf_client_id"),
        "client_secret": conf.get("stock_sync_cf_client_secret"),
        "warehouse": conf.get("stock_sync_warehouse", "Stores - Q"),
        "company": conf.get("stock_sync_company", "Qtechline"),
        # must be an Asset/Liability account: erpnext requires this for the first
        # ever Stock Reconciliation ("opening entry") against a company
        "expense_account": conf.get("stock_sync_expense_account", "Temporary Opening"),
        "cost_center": conf.get("stock_sync_cost_center", "Main"),
    }


def _headers(settings):
    return {
        "CF-Access-Client-Id": settings["client_id"],
        "CF-Access-Client-Secret": settings["client_secret"],
        "Accept": "application/json",
    }


def extract_doc_key(stock_no: str) -> str:
    """Strip the leading '<GROUP>-' prefix, mirroring the frontend's extractDocKey()."""
    for group in STOCK_GROUP_MAPPINGS:
        prefix = f"{group['code']}-"
        if stock_no.startswith(prefix):
            return stock_no[len(prefix):]
    return stock_no


def _item_group_code_for(stock_no: str) -> str:
    for group in STOCK_GROUP_MAPPINGS:
        if stock_no.startswith(f"{group['code']}-"):
            return group["code"]
    return "DEFAULT"


def fetch_stock_levels(settings, retries=3):
    # this payload is ~30MB for ~9800 items; latency is variable, so retry
    last_exc = None
    for _ in range(retries):
        try:
            resp = requests.get(
                f"{settings['api_base']}/assetStock", headers=_headers(settings), timeout=300
            )
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as exc:
            last_exc = exc
    raise last_exc


def fetch_sql_stock_qty(session, settings, stock_no, retries=2):
    doc_key = extract_doc_key(stock_no)
    last_exc = None
    for _ in range(retries):
        try:
            resp = session.get(
                f"{settings['api_base']}/stockitem/{doc_key}",
                headers=_headers(settings),
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json().get("balsqty")
        except requests.exceptions.RequestException as exc:
            last_exc = exc
    raise last_exc


def _ensure_item_group(code):
    if frappe.db.exists("Item Group", code):
        return code
    doc = frappe.new_doc("Item Group")
    doc.item_group_name = code
    doc.parent_item_group = "All Item Groups"
    doc.is_group = 0
    doc.insert(ignore_permissions=True)
    return doc.name


def _ensure_item(stock_no, stock_name):
    if frappe.db.exists("Item", stock_no):
        return
    item_group = _ensure_item_group(_item_group_code_for(stock_no))
    doc = frappe.new_doc("Item")
    doc.item_code = stock_no
    doc.item_name = stock_name or stock_no
    doc.item_group = item_group
    doc.stock_uom = "Nos"
    doc.is_stock_item = 1
    doc.insert(ignore_permissions=True)


def _abbr_suffixed(company, name):
    # site config may store either the bare name or the full "Name - ABBR" name
    if frappe.db.exists("Account", name) or frappe.db.exists("Cost Center", name):
        return name
    abbr = frappe.get_cached_value("Company", company, "abbr")
    return f"{name} - {abbr}"


def _submit_reconciliation(settings, rows):
    doc = frappe.new_doc("Stock Reconciliation")
    doc.company = settings["company"]
    doc.purpose = "Stock Reconciliation"
    doc.expense_account = _abbr_suffixed(settings["company"], settings["expense_account"])
    doc.cost_center = _abbr_suffixed(settings["company"], settings["cost_center"])
    for row in rows:
        doc.append("items", row)
    doc.insert(ignore_permissions=True)
    doc.submit()
    frappe.db.commit()
    return doc.name


@frappe.whitelist()
def sync_stock_from_sql():
    """Fetch stock levels + live SQL quantities from the external API and write
    them into Frappe as Items + chunked Stock Reconciliations."""
    frappe.only_for("System Manager")
    settings = _get_settings()
    if not settings["client_id"] or not settings["client_secret"]:
        frappe.throw(
            "stock_sync_cf_client_id / stock_sync_cf_client_secret are not set "
            "in site_config.json"
        )

    levels = fetch_stock_levels(settings)
    matched = [
        level
        for level in levels
        if level.get("stockNo")
        and any(level["stockNo"].startswith(p) for p in ALLOWED_PREFIXES)
    ]
    logger = frappe.logger("stock_sync")
    logger.info(f"stock_sync: {len(matched)}/{len(levels)} items matched allowed prefixes")

    for level in matched:
        _ensure_item(level["stockNo"], level.get("stockName"))
    frappe.db.commit()

    errors = []
    qtys = {}
    session = requests.Session()
    with ThreadPoolExecutor(max_workers=QTY_FETCH_WORKERS) as executor:
        futures = {
            executor.submit(fetch_sql_stock_qty, session, settings, level["stockNo"]): level["stockNo"]
            for level in matched
        }
        done = 0
        for future in as_completed(futures):
            stock_no = futures[future]
            done += 1
            try:
                qtys[stock_no] = future.result()
            except Exception:
                frappe.log_error(frappe.get_traceback(), f"stock_sync qty: {stock_no}")
                errors.append(stock_no)
            if done % 500 == 0:
                logger.info(f"stock_sync: fetched qty for {done}/{len(matched)} items")

    # negative qty is rejected outright by Stock Reconciliation and would fail
    # the whole chunk it's batched into, so drop it here instead
    rows = []
    for stock_no, qty in qtys.items():
        if qty is None:
            continue
        if qty < 0:
            errors.append(stock_no)
            continue
        rows.append({"item_code": stock_no, "warehouse": settings["warehouse"], "qty": float(qty)})

    reconciliations = []
    for start in range(0, len(rows), RECONCILE_CHUNK_SIZE):
        chunk = rows[start : start + RECONCILE_CHUNK_SIZE]
        try:
            reconciliations.append(_submit_reconciliation(settings, chunk))
        except Exception:
            frappe.log_error(frappe.get_traceback(), f"stock_sync reconcile chunk {start}")
            # one bad row shouldn't sink the other 199 - retry individually
            for row in chunk:
                try:
                    reconciliations.append(_submit_reconciliation(settings, [row]))
                except Exception:
                    frappe.log_error(frappe.get_traceback(), f"stock_sync reconcile item: {row['item_code']}")
                    errors.append(row["item_code"])
        logger.info(f"stock_sync: reconciled {min(start + RECONCILE_CHUNK_SIZE, len(rows))}/{len(rows)} items")

    return {"synced": len(rows) - len(errors), "reconciliations": reconciliations, "errors": errors}


@frappe.whitelist()
def enqueue_sync_stock_from_sql():
    """Kick sync_stock_from_sql off as a background job so an HTTP call from the
    UI doesn't time out waiting on thousands of items."""
    frappe.only_for("System Manager")
    frappe.enqueue(
        "stock_sync.stock_sync.sync.sync_stock_from_sql",
        queue="long",
        timeout=7200,
        job_name="stock_sync_from_sql",
    )
    return {"queued": True}


# --- Proxy endpoints ---------------------------------------------------
# These exist so the frontend talks only to Frappe (which holds the CF
# Access credentials in site_config.json) instead of calling the origin
# API directly with secrets baked into the JS bundle.


@frappe.whitelist()
def get_stock_levels():
    """Proxy for the origin API's /assetStock, used to populate the UI table."""
    frappe.only_for("System Manager")
    settings = _get_settings()
    return fetch_stock_levels(settings)


@frappe.whitelist()
def get_sql_stock_item(stock_no):
    """Proxy for the origin API's /stockitem/{docKey}, used by the edit modal."""
    frappe.only_for("System Manager")
    settings = _get_settings()
    session = requests.Session()
    return fetch_sql_stock_qty(session, settings, stock_no)


@frappe.whitelist()
def adjust_stock_item(stock_no, **payload):
    """Proxy for the origin API's /stockLevels/{stockNo}/adjust, used by the
    edit modal's Save button. Adjusts the origin SQL system; run
    sync_stock_from_sql afterwards to reflect the change in Frappe."""
    frappe.only_for("System Manager")
    settings = _get_settings()
    resp = requests.post(
        f"{settings['api_base']}/stockLevels/{stock_no}/adjust",
        headers={**_headers(settings), "Content-Type": "application/json"},
        json={**payload, "stockNo": stock_no},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


@frappe.whitelist()
def sync_new_stock_from_sql():
    """Proxy + handler for the origin API's /newSqlStock: ask it which stock
    codes were newly added, then create matching Items and an initial Stock
    Reconciliation for just those codes instead of re-syncing everything."""
    frappe.only_for("System Manager")
    settings = _get_settings()
    if not settings["client_id"] or not settings["client_secret"]:
        frappe.throw(
            "stock_sync_cf_client_id / stock_sync_cf_client_secret are not set "
            "in site_config.json"
        )

    resp = requests.get(
        f"{settings['api_base']}/newSqlStock", headers=_headers(settings), timeout=60
    )
    resp.raise_for_status()
    result = resp.json()
    new_ids = result.get("ids") or []
    if result.get("result") != "Success" or not new_ids:
        return {"result": result.get("result"), "ids": []}

    levels = fetch_stock_levels(settings)
    by_stock_no = {level["stockNo"]: level for level in levels if level.get("stockNo")}

    for stock_no in new_ids:
        level = by_stock_no.get(stock_no)
        if level:
            _ensure_item(stock_no, level.get("stockName"))
    frappe.db.commit()

    session = requests.Session()
    qtys = {}
    with ThreadPoolExecutor(max_workers=QTY_FETCH_WORKERS) as executor:
        futures = {
            executor.submit(fetch_sql_stock_qty, session, settings, stock_no): stock_no
            for stock_no in new_ids
            if stock_no in by_stock_no
        }
        for future in as_completed(futures):
            stock_no = futures[future]
            try:
                qtys[stock_no] = future.result()
            except Exception:
                frappe.log_error(frappe.get_traceback(), f"stock_sync new qty: {stock_no}")

    rows = [
        {"item_code": stock_no, "warehouse": settings["warehouse"], "qty": float(qty)}
        for stock_no, qty in qtys.items()
        if qty is not None
    ]
    if rows:
        _submit_reconciliation(settings, rows)

    return {"result": "Success", "ids": new_ids}
