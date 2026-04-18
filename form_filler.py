from __future__ import annotations

import argparse
import json
import math
import re
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import requests
from openpyxl import load_workbook
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys


TRACKING_DEFAULTS = {
    "status_column": "__status",
    "message_column": "__message",
    "processed_at_column": "__processed_at",
    "profile_column": "__profile_id",
}


@dataclass
class RowTask:
    row_number: int
    values: dict[str, str]


def log(message: str) -> None:
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


def normalize_key(value: str) -> str:
    collapsed = re.sub(r"\s+", " ", str(value or "").strip())
    return collapsed.lower()


def cell_to_string(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip()


def parse_selector(raw_selector: str) -> tuple[str, str]:
    selector = raw_selector.strip()
    selector_map = {
        "css=": By.CSS_SELECTOR,
        "xpath=": By.XPATH,
        "id=": By.ID,
        "name=": By.NAME,
    }
    for prefix, by in selector_map.items():
        if selector.startswith(prefix):
            return by, selector[len(prefix) :]
    if selector.startswith("//") or selector.startswith("(//"):
        return By.XPATH, selector
    return By.CSS_SELECTOR, selector


def load_config(config_path: Path) -> dict[str, Any]:
    if not config_path.exists():
        raise FileNotFoundError(
            f"Config file not found: {config_path}. Copy config.example.json to config.json and edit it first."
        )
    return json.loads(config_path.read_text(encoding="utf-8"))


def safe_save_workbook(workbook, path: Path) -> None:
    try:
        workbook.save(path)
    except PermissionError as exc:
        raise PermissionError(
            f"Cannot save Excel file '{path}'. Please close the Excel file and run the script again."
        ) from exc


class ExcelTracker:
    def __init__(self, excel_config: dict[str, Any]) -> None:
        self.path = Path(excel_config["path"]).expanduser().resolve()
        if not self.path.exists():
            raise FileNotFoundError(f"Excel file not found: {self.path}")

        self.header_row = int(excel_config.get("header_row", 1))
        self.first_data_row = int(excel_config.get("first_data_row", self.header_row + 1))
        self.sheet_name = excel_config.get("sheet_name")

        tracking = TRACKING_DEFAULTS | {
            key: excel_config.get(key, default)
            for key, default in TRACKING_DEFAULTS.items()
        }
        self.status_column_name = tracking["status_column"]
        self.message_column_name = tracking["message_column"]
        self.processed_at_column_name = tracking["processed_at_column"]
        self.profile_column_name = tracking["profile_column"]

        self.lock = threading.Lock()
        self.workbook = load_workbook(self.path)
        self.sheet = self.workbook[self.sheet_name] if self.sheet_name else self.workbook.active
        self.max_column = self.sheet.max_column
        self._header_index_by_normalized: dict[str, int] = {}
        self._header_index_by_exact: dict[str, int] = {}
        self._refresh_headers()
        self._ensure_tracking_columns()
        self._refresh_headers()
        self.source_header_names = self._build_source_header_names()

    def _refresh_headers(self) -> None:
        header_index_by_normalized: dict[str, int] = {}
        header_index_by_exact: dict[str, int] = {}
        max_col = self.sheet.max_column
        for col_idx in range(1, max_col + 1):
            header_value = self.sheet.cell(row=self.header_row, column=col_idx).value
            if header_value is None:
                continue
            exact = str(header_value).strip()
            if not exact:
                continue
            normalized = normalize_key(exact)
            if normalized in header_index_by_normalized:
                raise ValueError(
                    f"Duplicate Excel header detected after normalization: '{exact}'. Please rename duplicate columns."
                )
            header_index_by_normalized[normalized] = col_idx
            header_index_by_exact[exact] = col_idx
        self._header_index_by_normalized = header_index_by_normalized
        self._header_index_by_exact = header_index_by_exact

    def _ensure_tracking_columns(self) -> None:
        for column_name in (
            self.status_column_name,
            self.message_column_name,
            self.processed_at_column_name,
            self.profile_column_name,
        ):
            normalized = normalize_key(column_name)
            if normalized not in self._header_index_by_normalized:
                new_col_idx = self.sheet.max_column + 1
                self.sheet.cell(row=self.header_row, column=new_col_idx, value=column_name)
        safe_save_workbook(self.workbook, self.path)

    def _build_source_header_names(self) -> list[str]:
        tracking_names = {
            normalize_key(self.status_column_name),
            normalize_key(self.message_column_name),
            normalize_key(self.processed_at_column_name),
            normalize_key(self.profile_column_name),
        }
        headers: list[str] = []
        for exact_name in self._header_index_by_exact:
            if normalize_key(exact_name) not in tracking_names:
                headers.append(exact_name)
        return headers

    @property
    def available_headers(self) -> list[str]:
        return list(self.source_header_names)

    def build_pending_tasks(self) -> list[RowTask]:
        status_col_idx = self._header_index_by_normalized[normalize_key(self.status_column_name)]
        tasks: list[RowTask] = []
        for row_number in range(self.first_data_row, self.sheet.max_row + 1):
            row_values: dict[str, str] = {}
            is_empty = True
            for col_idx in range(1, self.sheet.max_column + 1):
                raw_value = self.sheet.cell(row=row_number, column=col_idx).value
                text_value = cell_to_string(raw_value)
                column_letter = self.sheet.cell(row=self.header_row, column=col_idx).column_letter.lower()
                row_values[f"__col_{column_letter}"] = text_value
                header_value = self.sheet.cell(row=self.header_row, column=col_idx).value
                if header_value is not None and str(header_value).strip():
                    row_values[normalize_key(str(header_value))] = text_value
                if text_value:
                    is_empty = False
            if is_empty:
                continue
            status_value = cell_to_string(self.sheet.cell(row=row_number, column=status_col_idx).value).upper()
            if status_value == "DONE":
                continue
            tasks.append(RowTask(row_number=row_number, values=row_values))
        return tasks

    def assert_column_exists(self, column_name: str) -> None:
        normalized = normalize_key(column_name)
        if normalized in self._header_index_by_normalized:
            return
        available = ", ".join(self.available_headers)
        raise ValueError(f"Excel column missing: {column_name}. Available columns: {available}")

    def assert_column_letter_exists(self, column_letter: str) -> None:
        normalized_letter = str(column_letter).strip().upper()
        if not normalized_letter or not normalized_letter.isalpha():
            raise ValueError(f"Invalid Excel column letter: {column_letter}")
        col_number = 0
        for char in normalized_letter:
            col_number = col_number * 26 + (ord(char) - 64)
        if col_number < 1 or col_number > self.sheet.max_column:
            raise ValueError(
                f"Excel column letter {column_letter} is outside the current sheet range (max column {self.sheet.max_column})."
            )

    def assert_field_sources_exist(self, fields: list[dict[str, Any]]) -> None:
        for field in fields:
            column_letter = str(field.get("excel_column_letter", "")).strip()
            if column_letter:
                self.assert_column_letter_exists(column_letter)
                continue
            self.assert_column_exists(field["column"])

    def assert_columns_exist(self, required_columns: list[str]) -> None:
        missing = [column for column in required_columns if normalize_key(column) not in self._header_index_by_normalized]
        if missing:
            available = ", ".join(self.available_headers)
            missing_text = ", ".join(missing)
            raise ValueError(f"Excel columns missing: {missing_text}. Available columns: {available}")

    def mark_result(self, row_number: int, status: str, message: str, profile_id: str) -> None:
        with self.lock:
            status_col = self._header_index_by_normalized[normalize_key(self.status_column_name)]
            message_col = self._header_index_by_normalized[normalize_key(self.message_column_name)]
            processed_at_col = self._header_index_by_normalized[normalize_key(self.processed_at_column_name)]
            profile_col = self._header_index_by_normalized[normalize_key(self.profile_column_name)]

            self.sheet.cell(row=row_number, column=status_col, value=status)
            self.sheet.cell(row=row_number, column=message_col, value=message[:30000])
            self.sheet.cell(
                row=row_number,
                column=processed_at_col,
                value=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
            self.sheet.cell(row=row_number, column=profile_col, value=profile_id)
            safe_save_workbook(self.workbook, self.path)


class AdsPowerClient:
    def __init__(self, adspower_config: dict[str, Any]) -> None:
        self.base_url = str(adspower_config.get("base_url", "http://local.adspower.net:50325")).rstrip("/")
        self.api_key = str(adspower_config.get("api_key", "")).strip()
        self.keep_browser_open = bool(adspower_config.get("keep_browser_open", False))

    def _headers(self) -> dict[str, str]:
        if not self.api_key:
            return {}
        return {"Authorization": f"Bearer {self.api_key}"}

    def _get_json(self, path: str, timeout: int = 60) -> dict[str, Any]:
        response = requests.get(f"{self.base_url}{path}", headers=self._headers(), timeout=timeout)
        response.raise_for_status()
        return response.json()

    def _connect_driver(self, browser_data: dict[str, Any]) -> webdriver.Chrome:
        chrome_driver_path = browser_data["webdriver"]
        debugger_address = browser_data["ws"]["selenium"]

        options = Options()
        options.add_experimental_option("debuggerAddress", debugger_address)
        service = Service(executable_path=chrome_driver_path)
        return webdriver.Chrome(service=service, options=options)

    def list_active_profiles(self) -> list[dict[str, Any]]:
        payload = self._get_json("/api/v1/browser/local-active", timeout=30)
        if payload.get("code") != 0:
            raise RuntimeError(f"AdsPower local-active failed: {payload.get('msg', 'unknown error')}")
        return payload.get("data", {}).get("list", [])

    def start_profile(self, profile_id: str) -> tuple[webdriver.Chrome, dict[str, Any]]:
        payload = self._get_json(f"/api/v1/browser/start?user_id={profile_id}", timeout=60)
        if payload.get("code") != 0:
            raise RuntimeError(f"AdsPower start failed for {profile_id}: {payload.get('msg', 'unknown error')}")
        driver = self._connect_driver(payload["data"])
        return driver, payload

    def attach_to_active_profile(self, profile_data: dict[str, Any]) -> webdriver.Chrome:
        return self._connect_driver(profile_data)

    def stop_profile(self, profile_id: str) -> None:
        if self.keep_browser_open:
            return
        response = requests.get(
            f"{self.base_url}/api/v1/browser/stop?user_id={profile_id}",
            headers=self._headers(),
            timeout=30,
        )
        response.raise_for_status()


def get_row_value(row_values: dict[str, str], column_name: str) -> str:
    return row_values.get(normalize_key(column_name), "")


def get_field_value(row_values: dict[str, str], field_config: dict[str, Any]) -> str:
    column_letter = str(field_config.get("excel_column_letter", "")).strip().lower()
    if column_letter:
        return row_values.get(f"__col_{column_letter}", "")
    return get_row_value(row_values, field_config["column"])


def parse_date_value(value: str) -> datetime:
    cleaned = value.strip()
    if not cleaned:
        raise ValueError("Date value is empty")

    date_formats = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%m/%d/%y",
        "%m-%d-%Y",
        "%m-%d-%y",
    )
    for fmt in date_formats:
        try:
            return datetime.strptime(cleaned, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unsupported date format: {value}")


def transform_value(raw_value: str, field_config: dict[str, Any]) -> str:
    transform = str(field_config.get("transform", "")).strip().lower()
    if not transform:
        return raw_value

    if transform in {"date_month_number", "date_day_number", "date_year"}:
        parsed = parse_date_value(raw_value)
        if transform == "date_month_number":
            return str(parsed.month)
        if transform == "date_day_number":
            return str(parsed.day)
        return str(parsed.year)

    raise ValueError(f"Unsupported transform '{transform}'")


def find_first_element(
    driver: webdriver.Chrome,
    selectors: list[str],
    timeout_seconds: int,
    require_displayed: bool = True,
):
    deadline = time.time() + timeout_seconds
    last_error = None
    while time.time() < deadline:
        for selector in selectors:
            by, query = parse_selector(selector)
            try:
                matches = driver.find_elements(by, query)
            except WebDriverException as exc:
                last_error = exc
                continue
            for element in matches:
                try:
                    if not require_displayed or element.is_displayed():
                        return element
                except WebDriverException as exc:
                    last_error = exc
        time.sleep(0.25)
    selector_text = " | ".join(selectors)
    if last_error:
        raise TimeoutException(f"Element not found for selectors: {selector_text}. Last error: {last_error}") from last_error
    raise TimeoutException(f"Element not found for selectors: {selector_text}")


def set_input_value(driver: webdriver.Chrome, element, value: str) -> None:
    driver.execute_script(
        """
        const element = arguments[0];
        const value = arguments[1];
        element.focus();
        element.value = value;
        element.dispatchEvent(new Event('input', { bubbles: true }));
        element.dispatchEvent(new Event('change', { bubbles: true }));
        """,
        element,
        value,
    )


def clear_and_type(driver: webdriver.Chrome, element, value: str) -> None:
    try:
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
        element.click()
        element.send_keys(Keys.CONTROL, "a")
        element.send_keys(Keys.DELETE)
        if value:
            element.send_keys(value)
        return
    except WebDriverException:
        set_input_value(driver, element, value)


def set_checkbox_state(driver: webdriver.Chrome, selectors: list[str], desired_state: bool, timeout_seconds: int) -> None:
    element = find_first_element(driver, selectors, timeout_seconds, require_displayed=False)
    try:
        is_checked = element.is_selected()
    except WebDriverException:
        is_checked = False
    if is_checked != desired_state:
        try:
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
            element.click()
        except WebDriverException:
            driver.execute_script(
                """
                arguments[0].checked = arguments[1];
                arguments[0].dispatchEvent(new Event('input', { bubbles: true }));
                arguments[0].dispatchEvent(new Event('change', { bubbles: true }));
                """,
                element,
                desired_state,
            )


def fill_field(driver: webdriver.Chrome, row_values: dict[str, str], field_config: dict[str, Any], timeout_seconds: int) -> None:
    column_name = field_config["column"]
    field_type = field_config.get("type", "text")
    selectors = field_config.get("selectors") or []
    if not selectors:
        raise ValueError(f"No selectors configured for Excel column '{column_name}'")

    raw_value = get_field_value(row_values, field_config)
    value = transform_value(raw_value, field_config)

    if field_type == "text":
        element = find_first_element(driver, selectors, timeout_seconds, require_displayed=True)
        clear_and_type(driver, element, value)
        return

    if field_type == "select_text":
        element = find_first_element(driver, selectors, timeout_seconds, require_displayed=True)
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
        if not value:
            return
        from selenium.webdriver.support.ui import Select

        select = Select(element)
        try:
            select.select_by_visible_text(value)
        except Exception:
            try:
                select.select_by_value(value)
            except Exception:
                driver.execute_script(
                    """
                    arguments[0].value = arguments[1];
                    arguments[0].dispatchEvent(new Event('input', { bubbles: true }));
                    arguments[0].dispatchEvent(new Event('change', { bubbles: true }));
                    """,
                    element,
                    value,
                )
        return

    if field_type == "checkbox":
        desired_state = value.lower() in {"1", "true", "yes", "y"}
        set_checkbox_state(driver, selectors, desired_state, timeout_seconds)
        return

    if field_type == "hidden_text":
        element = find_first_element(driver, selectors, timeout_seconds, require_displayed=False)
        driver.execute_script("arguments[0].value = arguments[1];", element, value)
        return

    raise ValueError(f"Unsupported field type '{field_type}' for column '{column_name}'")


def apply_static_checkboxes(driver: webdriver.Chrome, form_config: dict[str, Any], timeout_seconds: int) -> None:
    for checkbox in form_config.get("checkboxes", []):
        selectors = checkbox.get("selectors") or ([checkbox["selector"]] if checkbox.get("selector") else [])
        if not selectors:
            continue
        desired_state = bool(checkbox.get("checked", True))
        set_checkbox_state(driver, selectors, desired_state, timeout_seconds)


def submit_form(driver: webdriver.Chrome, form_config: dict[str, Any], timeout_seconds: int) -> None:
    submit_selector = str(form_config.get("submit_selector", "")).strip()
    if not submit_selector:
        raise ValueError("submit_after_fill is enabled but form.submit_selector is empty")

    button = find_first_element(driver, [submit_selector], timeout_seconds)
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
    try:
        button.click()
    except WebDriverException:
        driver.execute_script("arguments[0].click();", button)

    success_wait_selector = str(form_config.get("success_wait_selector", "")).strip()
    if success_wait_selector:
        find_first_element(driver, [success_wait_selector], timeout_seconds)
        return

    time.sleep(float(form_config.get("post_submit_wait_seconds", 3)))


def focus_browser_tab(driver: webdriver.Chrome, form_config: dict[str, Any]) -> None:
    handles = driver.window_handles
    if not handles:
        raise RuntimeError("No browser tabs are open in this AdsPower profile.")

    target_contains = str(form_config.get("target_url_contains", "")).strip().lower()
    if target_contains:
        for handle in reversed(handles):
            driver.switch_to.window(handle)
            if target_contains in driver.current_url.lower():
                return

    tab_index = int(form_config.get("tab_index", -1))
    try:
        target_handle = handles[tab_index]
    except IndexError as exc:
        raise ValueError(
            f"Configured form.tab_index={tab_index} but only {len(handles)} tab(s) are open in this profile."
        ) from exc

    driver.switch_to.window(target_handle)


def open_form(driver: webdriver.Chrome, form_config: dict[str, Any]) -> None:
    if form_config.get("use_existing_page", False):
        focus_browser_tab(driver, form_config)
        return

    target_url = str(form_config["url"]).strip()
    if not target_url:
        raise ValueError("form.url is empty in config and form.use_existing_page is false")
    page_load_timeout = int(form_config.get("page_load_timeout_seconds", 30))
    driver.set_page_load_timeout(page_load_timeout)
    driver.get(target_url)


def process_row(
    driver: webdriver.Chrome,
    row_task: RowTask,
    config: dict[str, Any],
) -> str:
    form_config = config["form"]
    fields = config["fields"]
    timeout_seconds = int(form_config.get("field_timeout_seconds", 15))

    if form_config.get("return_to_form_before_each_row", True):
        open_form(driver, form_config)

    for field_config in fields:
        fill_field(driver, row_task.values, field_config, timeout_seconds)

    apply_static_checkboxes(driver, form_config, timeout_seconds)

    if form_config.get("submit_after_fill", False):
        submit_form(driver, form_config, timeout_seconds)
        return "submitted"

    return "filled (submit disabled)"


def worker_loop(profile_target: dict[str, Any], assigned_tasks: list[RowTask], tracker: ExcelTracker, config: dict[str, Any]) -> None:
    client = AdsPowerClient(config["adspower"])
    retry_count = int(config.get("worker", {}).get("retry_count", 1))
    max_rows_per_profile = int(config.get("worker", {}).get("max_rows_per_profile", 0))
    profile_id = str(profile_target["user_id"])
    connect_mode = str(profile_target.get("connect_mode", "start"))

    driver = None
    processed_count = 0

    try:
        if connect_mode == "attach":
            log(f"[{profile_id}] Attaching to already open AdsPower browser")
            driver = client.attach_to_active_profile(profile_target["browser_data"])
        else:
            log(f"[{profile_id}] Starting AdsPower profile")
            driver, _ = client.start_profile(profile_id)

        if config["form"].get("return_to_form_before_each_row", True):
            open_form(driver, config["form"])

        for row_task in assigned_tasks:
            if max_rows_per_profile and processed_count >= max_rows_per_profile:
                log(f"[{profile_id}] Reached max_rows_per_profile={max_rows_per_profile}")
                break
            result_status = "FAILED"
            result_message = ""
            for attempt in range(1, retry_count + 2):
                try:
                    log(f"[{profile_id}] Processing Excel row {row_task.row_number} (attempt {attempt})")
                    result_message = process_row(driver, row_task, config)
                    result_status = "DONE"
                    break
                except Exception as exc:
                    result_message = str(exc)
                    if attempt <= retry_count:
                        log(
                            f"[{profile_id}] Row {row_task.row_number} failed on attempt {attempt}: {exc}. Retrying..."
                        )
                        time.sleep(2)
                    else:
                        log(f"[{profile_id}] Row {row_task.row_number} failed: {exc}")

            tracker.mark_result(row_task.row_number, result_status, result_message, profile_id)
            processed_count += 1

    except Exception as exc:
        log(f"[{profile_id}] Worker stopped: {exc}")
    finally:
        if driver is not None and connect_mode == "start" and not client.keep_browser_open:
            try:
                driver.quit()
            except Exception:
                pass
        if connect_mode == "start":
            try:
                client.stop_profile(profile_id)
            except Exception as exc:
                log(f"[{profile_id}] Could not stop AdsPower profile cleanly: {exc}")


def validate_config(config: dict[str, Any], tracker: ExcelTracker) -> None:
    if not config.get("fields"):
        raise ValueError("No fields configured in config.json")

    profile_ids = config.get("adspower", {}).get("profile_ids") or []
    use_active_profiles = bool(config.get("adspower", {}).get("use_active_profiles", True))
    if not profile_ids and not use_active_profiles:
        raise ValueError("adspower.profile_ids is empty")

    tracker.assert_field_sources_exist(config["fields"])

    transforms = {
        str(field.get("transform", "")).strip().lower()
        for field in config["fields"]
        if str(field.get("transform", "")).strip()
    }
    supported_transforms = {"date_month_number", "date_day_number", "date_year"}
    unsupported = transforms - supported_transforms
    if unsupported:
        raise ValueError(f"Unsupported transforms in config: {', '.join(sorted(unsupported))}")


def resolve_profile_targets(config: dict[str, Any]) -> list[dict[str, Any]]:
    client = AdsPowerClient(config["adspower"])
    explicit_profile_ids = [str(item).strip() for item in config["adspower"].get("profile_ids", []) if str(item).strip()]
    if explicit_profile_ids:
        return [{"user_id": profile_id, "connect_mode": "start"} for profile_id in explicit_profile_ids]

    if bool(config["adspower"].get("use_active_profiles", True)):
        active_profiles = client.list_active_profiles()
        if not active_profiles:
            raise RuntimeError(
                "No open AdsPower browsers found. Open the desired AdsPower browser profiles first, then run again."
            )
        return [
            {
                "user_id": str(profile_data["user_id"]),
                "connect_mode": "attach",
                "browser_data": profile_data,
            }
            for profile_data in active_profiles
        ]

    raise RuntimeError("No AdsPower profiles available to run.")


def build_profile_assignments(
    selected_profile_targets: list[dict[str, Any]],
    selected_tasks: list[RowTask],
    max_rows_per_profile: int,
) -> list[tuple[dict[str, Any], list[RowTask]]]:
    if not selected_profile_targets or not selected_tasks:
        return []

    assignments: list[tuple[dict[str, Any], list[RowTask]]] = []
    start_index = 0
    rows_per_profile = max_rows_per_profile if max_rows_per_profile > 0 else len(selected_tasks)
    for profile_target in selected_profile_targets:
        assigned = selected_tasks[start_index : start_index + rows_per_profile]
        if not assigned:
            break
        assignments.append((profile_target, assigned))
        start_index += rows_per_profile
    return assignments


def run(config_path: Path) -> None:
    config = load_config(config_path)
    tracker = ExcelTracker(config["excel"])
    validate_config(config, tracker)
    profile_targets = resolve_profile_targets(config)

    pending_tasks = tracker.build_pending_tasks()
    if not pending_tasks:
        log("No pending Excel rows found. Rows marked DONE are skipped automatically.")
        return

    max_rows_per_profile = int(config.get("worker", {}).get("max_rows_per_profile", 0))
    if max_rows_per_profile > 0:
        run_row_cap = len(profile_targets) * max_rows_per_profile
        selected_tasks = pending_tasks[:run_row_cap]
        workers_needed = math.ceil(len(selected_tasks) / max_rows_per_profile) if selected_tasks else 0
        selected_profile_targets = profile_targets[:workers_needed]
    else:
        selected_tasks = pending_tasks
        selected_profile_targets = profile_targets

    assignments = build_profile_assignments(selected_profile_targets, selected_tasks, max_rows_per_profile)

    log(
        f"Found {len(pending_tasks)} pending rows and {len(profile_targets)} AdsPower browser(s). "
        f"This run will process up to {len(selected_tasks)} row(s)."
    )

    threads: list[threading.Thread] = []
    for profile_target, assigned_tasks in assignments:
        thread = threading.Thread(
            target=worker_loop,
            args=(profile_target, assigned_tasks, tracker, config),
            daemon=False,
        )
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    refreshed_pending = tracker.build_pending_tasks()
    total_remaining = len(refreshed_pending)
    processed_in_batch = len(selected_tasks) - sum(
        1 for task in selected_tasks if any(p.row_number == task.row_number for p in refreshed_pending)
    )
    log(
        f"Batch finished. {processed_in_batch} row(s) were marked DONE in this run, "
        f"and {total_remaining} pending row(s) remain for the next run."
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Fill a web form from Excel rows across multiple AdsPower browsers.")
    parser.add_argument(
        "--config",
        default="config.json",
        help="Path to config JSON file. Default: config.json",
    )
    args = parser.parse_args()
    run(Path(args.config).expanduser().resolve())


if __name__ == "__main__":
    main()
