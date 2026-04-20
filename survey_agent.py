"""
Multi-Browser Survey Filling Agent for ADSPower.

Connects to all open ADSPower browsers and automatically answers survey
questions by selecting random options (radio buttons, checkboxes, buttons)
and clicking Continue/Next until the survey is complete.

Run this AFTER form_filler.py has filled and submitted the initial form.

Usage:
    python survey_agent.py                              # uses survey_config.json
    python survey_agent.py --config my_survey.json      # custom config
"""

from __future__ import annotations

import argparse
import enum
import hashlib
import json
import random
import re
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from selenium import webdriver
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    ElementNotInteractableException,
    NoSuchWindowException,
    StaleElementReferenceException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement

from form_filler import AdsPowerClient, log as _base_log


# ===================================================================
# Predefined Answers Loader
# ===================================================================

def normalize_text(text: str) -> str:
    """Normalize text for comparison: lowercase, punctuation-light, compact spaces."""
    cleaned = re.sub(r"[^a-z0-9\s]+", " ", str(text or "").lower())
    return " ".join(cleaned.split())


def load_predefined_answers(data_file: str = "data.txt") -> dict[str, str]:
    """
    Load predefined question-answer mappings from data.txt.
    Format: "question text? answer -> answer_value"
    
    Returns dict where key=normalized_question, value=answer_to_select
    """
    predefined = {}
    try:
        path = Path(data_file)
        if not path.exists():
            return predefined
        
        lines = path.read_text(encoding="utf-8").strip().split("\n")
        for line in lines:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            parts = re.split(r"\s+answer\s*->\s*", line, maxsplit=1, flags=re.IGNORECASE)
            if len(parts) == 2:
                question = normalize_text(parts[0])
                answer = parts[1].strip()
                predefined[question] = answer
                log(f"Loaded predefined answer: '{question[:50]}...' -> '{answer}'")
    except Exception as e:
        log(f"Warning: Could not load predefined answers: {e}")
    
    return predefined


class SurveyLearningStore:
    """Persistent memory for answers that worked or failed in previous runs."""

    def __init__(self, memory_file: str = "survey_memory.json") -> None:
        self.path = Path(memory_file)
        self.lock = threading.Lock()
        self.data = self._load()

    def _default_data(self) -> dict[str, Any]:
        return {
            "learned_answers": {},
            "page_memory": {},
            "recent_failures": [],
            "profile_memory": {},
        }

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return self._default_data()
        try:
            loaded = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                return self._default_data() | loaded
        except Exception:
            pass
        return self._default_data()

    def _save_locked(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            json.dumps(self.data, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    @staticmethod
    def _question_key(question_text: str) -> str:
        return normalize_text(question_text)[:600]

    @staticmethod
    def _answer_key(answer_text: str) -> str:
        return normalize_text(answer_text)[:200]

    def build_page_signature(self, question_text: str, option_texts: list[str]) -> str:
        normalized_question = self._question_key(question_text)
        normalized_options = [self._answer_key(text) for text in option_texts[:12]]
        payload = " || ".join([normalized_question] + normalized_options)
        return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:20]

    def get_learned_answer(
        self,
        question_text: str,
        option_texts: list[str],
        page_signature: str,
    ) -> str | None:
        question_key = self._question_key(question_text)
        normalized_options = [self._answer_key(text) for text in option_texts if self._answer_key(text)]
        if not normalized_options:
            return None

        with self.lock:
            question_memory = self.data.get("learned_answers", {}).get(question_key, {})
            page_memory = self.data.get("page_memory", {}).get(page_signature, {})
            page_success = page_memory.get("success_answers", {})
            page_failure = page_memory.get("failed_answers", {})
            preferred_answer = self._answer_key(page_memory.get("preferred_answer", ""))

        best_option: str | None = None
        best_score = 0

        for option in normalized_options:
            score = 0
            q_stats = question_memory.get(option, {})
            score += int(q_stats.get("success", 0)) * 5
            score -= int(q_stats.get("failure", 0)) * 6
            score += int(page_success.get(option, 0)) * 4
            score -= int(page_failure.get(option, 0)) * 7
            if preferred_answer and option == preferred_answer:
                score += 3

            if score > best_score:
                best_score = score
                best_option = option

        return best_option if best_score > 0 else None

    def remember_success(
        self,
        question_text: str,
        answer_text: str,
        option_texts: list[str],
        page_signature: str,
        source: str,
    ) -> None:
        question_key = self._question_key(question_text)
        answer_key = self._answer_key(answer_text)
        if not question_key or not answer_key:
            return

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self.lock:
            learned_answers = self.data.setdefault("learned_answers", {})
            question_bucket = learned_answers.setdefault(question_key, {})
            answer_bucket = question_bucket.setdefault(
                answer_key,
                {"answer_text": answer_text, "success": 0, "failure": 0},
            )
            answer_bucket["answer_text"] = answer_text
            answer_bucket["success"] = int(answer_bucket.get("success", 0)) + 1
            answer_bucket["last_result"] = "success"
            answer_bucket["last_source"] = source
            answer_bucket["last_seen_at"] = timestamp

            page_memory = self.data.setdefault("page_memory", {})
            page_bucket = page_memory.setdefault(
                page_signature,
                {
                    "question_text": question_key,
                    "option_texts": option_texts[:12],
                    "success_answers": {},
                    "failed_answers": {},
                },
            )
            page_bucket["question_text"] = question_key
            page_bucket["option_texts"] = option_texts[:12]
            page_bucket["preferred_answer"] = answer_text
            page_bucket["last_result"] = "success"
            page_bucket["last_seen_at"] = timestamp
            success_answers = page_bucket.setdefault("success_answers", {})
            success_answers[answer_key] = int(success_answers.get(answer_key, 0)) + 1

            self._save_locked()

    def remember_failure(
        self,
        question_text: str,
        answer_text: str,
        option_texts: list[str],
        page_signature: str,
        reason: str,
        source: str,
    ) -> None:
        question_key = self._question_key(question_text)
        answer_key = self._answer_key(answer_text)
        if not question_key or not answer_key:
            return

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self.lock:
            learned_answers = self.data.setdefault("learned_answers", {})
            question_bucket = learned_answers.setdefault(question_key, {})
            answer_bucket = question_bucket.setdefault(
                answer_key,
                {"answer_text": answer_text, "success": 0, "failure": 0},
            )
            answer_bucket["answer_text"] = answer_text
            answer_bucket["failure"] = int(answer_bucket.get("failure", 0)) + 1
            answer_bucket["last_result"] = "failure"
            answer_bucket["last_source"] = source
            answer_bucket["last_seen_at"] = timestamp
            answer_bucket["last_reason"] = reason[:400]

            page_memory = self.data.setdefault("page_memory", {})
            page_bucket = page_memory.setdefault(
                page_signature,
                {
                    "question_text": question_key,
                    "option_texts": option_texts[:12],
                    "success_answers": {},
                    "failed_answers": {},
                },
            )
            page_bucket["question_text"] = question_key
            page_bucket["option_texts"] = option_texts[:12]
            page_bucket["last_result"] = "failure"
            page_bucket["last_seen_at"] = timestamp
            page_bucket["last_reason"] = reason[:400]
            failed_answers = page_bucket.setdefault("failed_answers", {})
            failed_answers[answer_key] = int(failed_answers.get(answer_key, 0)) + 1

            recent_failures = self.data.setdefault("recent_failures", [])
            recent_failures.insert(
                0,
                {
                    "timestamp": timestamp,
                    "question_text": question_key,
                    "answer_text": answer_text,
                    "option_texts": option_texts[:12],
                    "reason": reason[:400],
                    "source": source,
                    "page_signature": page_signature,
                },
            )
            del recent_failures[200:]

            self._save_locked()

    def note_page_issue(
        self,
        question_text: str,
        option_texts: list[str],
        page_signature: str,
        reason: str,
    ) -> None:
        question_key = self._question_key(question_text)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self.lock:
            page_memory = self.data.setdefault("page_memory", {})
            page_bucket = page_memory.setdefault(
                page_signature,
                {
                    "question_text": question_key,
                    "option_texts": option_texts[:12],
                    "success_answers": {},
                    "failed_answers": {},
                },
            )
            page_bucket["question_text"] = question_key
            page_bucket["option_texts"] = option_texts[:12]
            page_bucket["last_result"] = "issue"
            page_bucket["last_seen_at"] = timestamp
            page_bucket["last_reason"] = reason[:400]
            self._save_locked()

    def get_profile_priority(self, profile_id: str) -> int:
        profile_key = str(profile_id).strip()
        if not profile_key:
            return 0
        with self.lock:
            stats = self.data.get("profile_memory", {}).get(profile_key, {})
        priority = 0
        priority += int(stats.get("no_result_count", 0)) * 10
        priority += int(stats.get("connect_failure_count", 0)) * 8
        priority += int(stats.get("thread_crash_count", 0)) * 7
        priority += int(stats.get("failed_count", 0)) * 4
        priority += int(stats.get("stuck_count", 0)) * 3
        priority -= int(stats.get("completed_count", 0))
        return priority

    def get_profile_attach_retry_count(self, profile_id: str, base_retries: int) -> int:
        profile_key = str(profile_id).strip()
        retries = max(1, int(base_retries))
        if not profile_key:
            return retries
        with self.lock:
            stats = self.data.get("profile_memory", {}).get(profile_key, {})
        penalty = (
            int(stats.get("no_result_count", 0))
            + int(stats.get("connect_failure_count", 0))
            + int(stats.get("thread_crash_count", 0))
        )
        return min(retries + penalty, retries + 4)

    def get_discovery_retry_bonus(self) -> int:
        with self.lock:
            profile_memory = self.data.get("profile_memory", {})
        if not profile_memory:
            return 0
        total_problem_profiles = 0
        for stats in profile_memory.values():
            issue_score = (
                int(stats.get("no_result_count", 0))
                + int(stats.get("connect_failure_count", 0))
                + int(stats.get("thread_crash_count", 0))
            )
            if issue_score > 0:
                total_problem_profiles += 1
        return min(total_problem_profiles, 3)

    def note_profile_result(self, profile_id: str, outcome: str, message: str = "") -> None:
        profile_key = str(profile_id).strip()
        if not profile_key:
            return

        outcome_key = normalize_text(outcome).replace(" ", "_") or "unknown"
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self.lock:
            profile_memory = self.data.setdefault("profile_memory", {})
            stats = profile_memory.setdefault(
                profile_key,
                {
                    "completed_count": 0,
                    "disqualified_count": 0,
                    "stuck_count": 0,
                    "failed_count": 0,
                    "connect_failure_count": 0,
                    "thread_crash_count": 0,
                    "no_result_count": 0,
                },
            )

            count_field_map = {
                "completed": "completed_count",
                "disqualified": "disqualified_count",
                "stuck": "stuck_count",
                "failed": "failed_count",
                "connect_failed": "connect_failure_count",
                "thread_crashed": "thread_crash_count",
                "no_result": "no_result_count",
            }
            count_field = count_field_map.get(outcome_key, "failed_count")
            stats[count_field] = int(stats.get(count_field, 0)) + 1
            stats["last_outcome"] = outcome_key
            stats["last_message"] = str(message or "")[:400]
            stats["last_seen_at"] = timestamp

            if outcome_key in {"completed", "disqualified"}:
                stats["no_result_count"] = 0
                stats["connect_failure_count"] = 0
                stats["thread_crash_count"] = 0

            self._save_locked()


# ===================================================================
# Constants — selectors for survey elements
# ===================================================================

# Continue / Next / Submit button selectors (CSS)
CONTINUE_BUTTON_CSS: list[str] = [
    "input[type='submit']",
    "button[type='submit']",
]

# Continue / Next buttons by text (XPath) — case-insensitive
CONTINUE_BUTTON_XPATH: list[str] = [
    "//button[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'continue')]",
    "//button[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'next')]",
    "//button[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'submit')]",
    "//button[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'proceed')]",
    "//button[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'done')]",
    "//button[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'ok')]",
    "//a[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'continue')]",
    "//a[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'next')]",
    "//input[contains(translate(@value,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'continue')]",
    "//input[contains(translate(@value,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'next')]",
    "//input[contains(translate(@value,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'submit')]",
    # Arrows / icon-only buttons commonly used for "Next"
    "//button[contains(@class,'next')]",
    "//button[contains(@class,'continue')]",
    "//a[contains(@class,'next')]",
    "//a[contains(@class,'continue')]",
]

NON_CONTENT_URL_PREFIXES = (
    "devtools://",
    "chrome://",
    "chrome-extension://",
    "about:",
    "data:",
)

INTERACTIVE_SIGNAL_SELECTORS: tuple[str, ...] = (
    "input[type='radio']",
    "input[type='checkbox']",
    "select",
    "textarea",
    "input[type='text']",
    "input[type='email']",
    "input[type='tel']",
    "input[type='number']",
    "[role='radio']",
    "[role='checkbox']",
    "[role='option']",
    "[role='button'][data-value]",
    "label[for]",
)

DEFAULT_WEAK_COMPLETION_KEYWORDS = [
    "your reward",
    "you will receive",
    "points have been",
]

TERMINAL_CLAIM_BUTTON_KEYWORDS = (
    "claim",
    "redeem",
    "collect",
)

TERMINAL_CLAIM_PAGE_HINTS = (
    "claim your",
    "deal below",
    "paid offers",
    "pending",
    "prize",
    "reward",
    "earn save",
)

PROMOTIONAL_OPT_OUT_HINTS = (
    "please answer the following questions",
    "sign up",
    "receive email",
    "receive an email",
    "exclusive offers",
    "save $",
    "save 2 00",
    "powered by",
    "discover nexxus",
    "hellmann",
    "unilever",
    "axe",
    "game day",
    "prizes",
)

UNKNOWN_QUESTION_FALLBACK_PHRASES = (
    "none of the above",
    "none of these",
    "none apply",
    "none",
    "not applicable",
    "prefer not to answer",
    "do not wish to answer",
)

UNKNOWN_QUESTION_NO_EXCLUSIONS = (
    "no preference",
    "no opinion",
    "no difference",
)


# ===================================================================
# Enums & data classes
# ===================================================================

class SurveyState(enum.Enum):
    """State of the survey for a given browser."""
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    DISQUALIFIED = "disqualified"
    STUCK = "stuck"
    FAILED = "failed"


class QuestionType(enum.Enum):
    """Type of survey question detected on the current page."""
    RADIO = "radio"
    CHECKBOX = "checkbox"
    BUTTON_OPTIONS = "button_options"
    DROPDOWN = "dropdown"
    TEXT_INPUT = "text_input"
    NONE = "none"


@dataclass
class SurveyResult:
    """Final result for one browser's survey run."""
    profile_id: str
    state: str = SurveyState.IN_PROGRESS.value
    questions_answered: int = 0
    url: str = ""
    title: str = ""
    message: str = ""
    timestamp: str = ""
    screenshot_path: str = ""

    def refresh_timestamp(self) -> None:
        self.timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ===================================================================
# Thread-safe logging
# ===================================================================

_log_lock = threading.Lock()
_log_records: list[dict[str, Any]] = []


def log(message: str, profile_id: str = "") -> None:
    prefix = f"[{profile_id}] " if profile_id else ""
    # Safely encode for Windows console (cp1252) by replacing un-encodable chars
    safe_msg = f"{prefix}{message}"
    try:
        _base_log(safe_msg)
    except UnicodeEncodeError:
        _base_log(safe_msg.encode("ascii", errors="replace").decode("ascii"))


def record_result(result: SurveyResult) -> None:
    result.refresh_timestamp()
    with _log_lock:
        _log_records.append(asdict(result))


def save_log_file(log_directory: str) -> Path:
    log_dir = Path(log_directory)
    log_dir.mkdir(parents=True, exist_ok=True)
    filename = f"survey_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.json"
    log_path = log_dir / filename
    with _log_lock:
        log_path.write_text(
            json.dumps(_log_records, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    return log_path


# ===================================================================
# Screenshot helper
# ===================================================================

def capture_screenshot(
    driver: webdriver.Chrome,
    profile_id: str,
    label: str,
    screenshot_directory: str,
) -> str:
    ss_dir = Path(screenshot_directory)
    ss_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{profile_id}_{label}_{timestamp}.png"
    filepath = ss_dir / filename
    try:
        driver.save_screenshot(str(filepath))
        log(f"Screenshot saved: {filepath}", profile_id)
    except WebDriverException as exc:
        log(f"Screenshot failed: {exc}", profile_id)
        return ""
    return str(filepath)


# ===================================================================
# Element interaction helpers
# ===================================================================

def safe_click(driver: webdriver.Chrome, element: WebElement) -> bool:
    """Click an element with fallbacks for intercepted/non-interactable cases."""
    try:
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
        time.sleep(0.3)
    except WebDriverException:
        pass

    try:
        element.click()
        return True
    except (ElementClickInterceptedException, ElementNotInteractableException):
        pass
    except WebDriverException:
        pass

    # Fallback: JS click
    try:
        driver.execute_script("arguments[0].click();", element)
        return True
    except WebDriverException:
        pass

    return False


def get_visible_elements(driver: webdriver.Chrome, by: str, selector: str) -> list[WebElement]:
    """Return only visible, enabled elements matching the selector."""
    try:
        elements = driver.find_elements(by, selector)
        visible = []
        for el in elements:
            try:
                if el.is_displayed() and el.is_enabled():
                    visible.append(el)
            except (StaleElementReferenceException, WebDriverException):
                continue
        return visible
    except WebDriverException:
        return []


def get_page_text(driver: webdriver.Chrome) -> str:
    """Return the full visible text of the page body (lowercase)."""
    try:
        text = driver.execute_script(
            "return document.body ? document.body.innerText : '';"
        )
        return (text or "").lower()
    except WebDriverException:
        return ""


# ===================================================================
# QuestionDetector — finds what type of question is on screen
# ===================================================================

class QuestionDetector:
    """Detects the type of survey question on the current page."""

    def detect(self, driver: webdriver.Chrome) -> tuple[QuestionType, list[WebElement]]:
        """
        Detect the question type and return the selectable option elements.

        Returns (QuestionType, list_of_option_elements).
        """
        # 1) Radio buttons
        radios = get_visible_elements(driver, By.CSS_SELECTOR, "input[type='radio']")
        if radios:
            return QuestionType.RADIO, radios

        # 2) Checkboxes
        checkboxes = get_visible_elements(driver, By.CSS_SELECTOR, "input[type='checkbox']")
        if checkboxes:
            return QuestionType.CHECKBOX, checkboxes

        # 3) Dropdown / select
        selects = get_visible_elements(driver, By.CSS_SELECTOR, "select")
        if selects:
            return QuestionType.DROPDOWN, selects

        # 4) Clickable answer buttons — divs/spans/labels that look like
        #    answer options (common in modern surveys like Qualtrics, etc.)
        button_options = self._find_answer_buttons(driver)
        if button_options:
            return QuestionType.BUTTON_OPTIONS, button_options

        # 5) Text inputs (textarea or text input not already filled)
        text_inputs = self._find_empty_text_inputs(driver)
        if text_inputs:
            return QuestionType.TEXT_INPUT, text_inputs

        return QuestionType.NONE, []

    def _find_answer_buttons(self, driver: webdriver.Chrome) -> list[WebElement]:
        """
        Find clickable answer-option elements that aren't standard inputs.
        These are often styled divs, labels, or list items in surveys.
        """
        # Phase 1: Known semantic selectors
        selectors_phase1 = [
            # Labels wrapping radio/checkbox (clickable labels)
            "label[for]",
            # Common survey answer wrappers
            "[role='radio']",
            "[role='checkbox']",
            "[role='option']",
            "[role='button'][data-value]",
            # Common survey platform classes
            ".answer-option",
            ".survey-option",
            ".choice",
            ".option-item",
            ".response-option",
            # Qualtrics-style
            ".q-radio",
            ".q-checkbox",
            # SurveyMonkey-style
            ".radio-button-label",
            ".checkbox-button-label",
        ]
        for css in selectors_phase1:
            elements = get_visible_elements(driver, By.CSS_SELECTOR, css)
            if len(elements) >= 2:  # At least 2 options = likely answer choices
                return elements

        # Phase 2: Look for groups of similarly-styled clickable elements
        # (like the red/orange buttons in the screenshot)
        try:
            candidates = driver.execute_script("""
                // Find groups of sibling clickable elements that look like answer buttons
                var candidates = [];
                
                // Strategy 1: Look for multiple sibling <a> or <button> or <div> with same parent
                // that have text content and similar styling (likely answer options)
                var containers = document.querySelectorAll('div, ul, ol, fieldset, form, section, main');
                for (var i = 0; i < containers.length; i++) {
                    var parent = containers[i];
                    // Find direct child clickable elements
                    var clickable = [];
                    var children = parent.children;
                    for (var j = 0; j < children.length; j++) {
                        var child = children[j];
                        var tag = child.tagName.toLowerCase();
                        var text = (child.innerText || '').trim();
                        var rect = child.getBoundingClientRect();
                        // Must be visible, have text, reasonable size
                        if (text && text.length > 0 && text.length < 200 &&
                            rect.width > 30 && rect.height > 15 &&
                            rect.top >= 0 && rect.top < window.innerHeight + 500) {
                            var style = window.getComputedStyle(child);
                            var cursor = style.cursor;
                            var hasClick = child.onclick !== null || child.hasAttribute('onclick');
                            var isLink = tag === 'a';
                            var isBtnLike = tag === 'button' || child.getAttribute('role') === 'button';
                            var hasBg = style.backgroundColor && style.backgroundColor !== 'rgba(0, 0, 0, 0)' && style.backgroundColor !== 'transparent';
                            var hasBorder = style.borderWidth && style.borderWidth !== '0px';
                            if (cursor === 'pointer' || hasClick || isLink || isBtnLike || hasBg || hasBorder) {
                                clickable.push(child);
                            }
                        }
                    }
                    // If we found 2+ similar clickable children, these are likely answer options
                    if (clickable.length >= 2 && clickable.length <= 15) {
                        // Verify they look similar (same tag, similar height)
                        var firstTag = clickable[0].tagName;
                        var sameTag = clickable.every(function(el) { return el.tagName === firstTag; });
                        if (sameTag || clickable.length >= 3) {
                            return clickable;
                        }
                    }
                }
                return [];
            """)
            if candidates and len(candidates) >= 2:
                return candidates
        except WebDriverException:
            pass

        return []

    def _find_empty_text_inputs(self, driver: webdriver.Chrome) -> list[WebElement]:
        """Find visible text inputs/textareas that are still empty."""
        inputs: list[WebElement] = []
        for css in ("input[type='text']", "input[type='number']", "textarea"):
            for el in get_visible_elements(driver, By.CSS_SELECTOR, css):
                try:
                    val = el.get_attribute("value") or ""
                    if not val.strip():
                        inputs.append(el)
                except WebDriverException:
                    continue
        return inputs


# ===================================================================
# SurveyWorker — per-browser survey answering logic
# ===================================================================

class SurveyWorker:
    """
    Runs in its own thread. Answers survey questions on one browser
    by selecting random options and clicking Continue/Next.
    """

    def __init__(
        self,
        driver: webdriver.Chrome,
        profile_id: str,
        config: dict[str, Any],
        predefined_answers: dict[str, str] | None = None,
        learning_store: SurveyLearningStore | None = None,
    ) -> None:
        self.driver = driver
        self.profile_id = profile_id
        self.detector = QuestionDetector()
        self.predefined_answers = predefined_answers or {}
        self.learning_store = learning_store

        self.max_questions: int = int(config.get("max_questions", 50))
        self.wait_after_click: float = float(config.get("wait_after_click_seconds", 3))
        self.element_timeout: int = int(config.get("element_timeout_seconds", 15))
        self.max_stuck_retries: int = int(config.get("max_stuck_retries", 5))
        self.stuck_wait: float = float(config.get("stuck_wait_seconds", 5))
        self.screenshot_enabled: bool = bool(config.get("screenshot_on_completion", True))
        self.screenshot_dir: str = str(config.get("screenshot_directory", "screenshots"))
        self.tab_index: int = int(config.get("tab_index", -1))
        self.target_url_contains: str = str(config.get("target_url_contains", "")).strip().lower()
        self.lock_current_tab_on_start: bool = bool(config.get("lock_current_tab_on_start", True))
        self.completion_keywords: list[str] = [
            kw.lower() for kw in config.get("completion_keywords", [])
        ]
        self.weak_completion_keywords: list[str] = [
            kw.lower()
            for kw in config.get(
                "weak_completion_keywords",
                DEFAULT_WEAK_COMPLETION_KEYWORDS,
            )
        ]
        self.skip_keywords: list[str] = [
            kw.lower() for kw in config.get("skip_keywords", [])
        ]
        self.strict_predefined_questions: set[str] = {
            normalize_text(question)
            for question in config.get("strict_predefined_questions", [])
            if normalize_text(question)
        }
        self._required_predefined_answer_missing = False
        self._required_predefined_message = ""
        self._survey_tab_handle: str | None = None
        self._current_question_context: dict[str, Any] | None = None
        self._pending_answer_record: dict[str, Any] | None = None
        self._session_answer_trail: list[dict[str, Any]] = []
        self._learning_finalized = False

        self.result = SurveyResult(profile_id=self.profile_id)

    # ---- helpers --------------------------------------------------------

    def _focus_correct_tab(self) -> None:
        """Keep the worker pinned to one survey tab and avoid cross-tab scanning."""
        try:
            handles = self.driver.window_handles
            if not handles:
                return

            try:
                original_handle = self.driver.current_window_handle
            except WebDriverException:
                original_handle = None

            def switch_to(handle: str, reason: str) -> None:
                self.driver.switch_to.window(handle)
                self._survey_tab_handle = handle
                if handle != original_handle:
                    log(f"Focused {reason}: {self.driver.current_url[:80]}", self.profile_id)

            if self._survey_tab_handle and self._survey_tab_handle in handles:
                if original_handle != self._survey_tab_handle:
                    switch_to(self._survey_tab_handle, "locked survey tab")
                return

            # 1) If user specified a target URL substring, prefer that tab
            if self.target_url_contains:
                for handle in reversed(handles):
                    self.driver.switch_to.window(handle)
                    try:
                        url = self.driver.current_url.lower()
                        if self.target_url_contains in url and self._is_content_url(url):
                            switch_to(handle, "tab by target_url_contains")
                            return
                    except WebDriverException:
                        continue

            # 2) Respect an explicit non-negative tab index if one is configured.
            if self.tab_index >= 0:
                try:
                    indexed_handle = handles[self.tab_index]
                    self.driver.switch_to.window(indexed_handle)
                    indexed_url = self.driver.current_url.lower()
                    if self._is_content_url(indexed_url):
                        switch_to(indexed_handle, f"configured tab_index={self.tab_index}")
                        return
                except (IndexError, WebDriverException):
                    pass

            # 3) By default, lock the currently active content tab and never scan others.
            if self.lock_current_tab_on_start and original_handle and original_handle in handles:
                try:
                    current_url = self.driver.current_url.lower()
                    if self._is_content_url(current_url):
                        self._survey_tab_handle = original_handle
                        return
                except WebDriverException:
                    pass

            # 4) Last-resort fallback only if the current tab is not usable.
            for handle in reversed(handles):
                try:
                    self.driver.switch_to.window(handle)
                    current_url = self.driver.current_url.lower()
                    if self._is_content_url(current_url):
                        switch_to(handle, "content tab fallback")
                        return
                except WebDriverException:
                    continue

            log("WARNING: No usable content tab found for survey worker", self.profile_id)

        except WebDriverException as exc:
            log(f"Tab focus failed: {exc}", self.profile_id)

    @staticmethod
    def _is_content_url(url: str) -> bool:
        normalized = (url or "").strip().lower()
        if not normalized:
            return False
        return not any(normalized.startswith(prefix) for prefix in NON_CONTENT_URL_PREFIXES)

    def _get_current_tab_match_score(self, current_url: str) -> int:
        """Score the currently selected tab based on how likely it is to be the survey tab."""
        score = 0
        url = (current_url or "").strip().lower()
        page_text = get_page_text(self.driver)
        title = ""
        try:
            title = (self.driver.title or "").strip().lower()
        except WebDriverException:
            title = ""

        if self.target_url_contains and self.target_url_contains in url:
            score += 100

        if self._page_has_interaction_signal():
            score += 40

        try:
            claim_button, _ = self._detect_terminal_claim_screen()
        except WebDriverException:
            claim_button = None
        if claim_button is not None:
            score += 80

        try:
            if self._is_completed():
                score += 60
        except WebDriverException:
            pass

        try:
            if self._is_disqualified():
                score += 30
        except WebDriverException:
            pass

        survey_hints = (
            "survey",
            "question",
            "stimulus assistant",
            "claim 1 deal below",
            "paid offers",
            "continue",
            "next",
            "submit",
            "yes",
            "no",
        )
        hint_hits = sum(1 for hint in survey_hints if hint in page_text or hint in title or hint in url)
        score += min(hint_hits, 6) * 5

        return score

    def _page_has_interaction_signal(self) -> bool:
        if self._find_continue_button() is not None:
            return True

        try:
            return bool(
                self.driver.execute_script(
                    """
                    const selectors = arguments[0];
                    return selectors.some((selector) =>
                        Array.from(document.querySelectorAll(selector)).some((el) => {
                            const rect = el.getBoundingClientRect();
                            const style = window.getComputedStyle(el);
                            return rect.width > 0
                                && rect.height > 0
                                && style.display !== 'none'
                                && style.visibility !== 'hidden';
                        })
                    );
                    """,
                    list(INTERACTIVE_SIGNAL_SELECTORS),
                )
            )
        except WebDriverException:
            return False

    def _update_result(self, state: SurveyState, message: str = "") -> None:
        try:
            self.result.url = self.driver.current_url
        except WebDriverException:
            self.result.url = "(unreachable)"
        try:
            self.result.title = self.driver.title
        except WebDriverException:
            self.result.title = "(unreachable)"
        if state != SurveyState.IN_PROGRESS:
            self._finalize_learning(state, message)
        self.result.state = state.value
        self.result.message = message
        record_result(self.result)

    def _screenshot(self, label: str) -> None:
        if self.screenshot_enabled:
            path = capture_screenshot(
                self.driver, self.profile_id, label, self.screenshot_dir
            )
            self.result.screenshot_path = path

    def _is_completed(self) -> bool:
        """Check if the survey is done (thank you / completion page)."""
        page_text = get_page_text(self.driver)
        matched = [kw for kw in self.completion_keywords if kw in page_text]
        if not matched:
            return False

        strong_matches = [
            kw for kw in matched if kw not in set(self.weak_completion_keywords)
        ]
        if self._page_has_interaction_signal():
            return False

        return bool(strong_matches or matched)

    def _is_disqualified(self) -> bool:
        """Check if screened out / disqualified."""
        page_text = get_page_text(self.driver)
        if not any(kw in page_text for kw in self.skip_keywords):
            return False
        return not self._page_has_interaction_signal()

    def _get_clickable_text_candidates(self) -> list[WebElement]:
        """Collect visible clickable elements that expose useful text for matching."""
        try:
            candidates = self.driver.execute_script(
                """
                const selectors = [
                    "button",
                    "a",
                    "input[type='button']",
                    "input[type='submit']",
                    "input[type='radio']",
                    "input[type='checkbox']",
                    "[role='button']",
                    "[role='radio']",
                    "[role='checkbox']",
                    "[role='option']",
                    "label",
                    "div",
                    "span",
                    "li"
                ];

                const isVisible = (el) => {
                    const rect = el.getBoundingClientRect();
                    const style = window.getComputedStyle(el);
                    return rect.width > 20
                        && rect.height > 16
                        && style.display !== "none"
                        && style.visibility !== "hidden"
                        && style.opacity !== "0";
                };

                const hasUsefulText = (el) => {
                    const text = (
                        el.innerText
                        || el.textContent
                        || el.value
                        || el.getAttribute("aria-label")
                        || el.getAttribute("title")
                        || ""
                    ).trim();
                    return text.length > 0 && text.length <= 250;
                };

                const isClickable = (el) => {
                    const tag = (el.tagName || "").toLowerCase();
                    const role = (el.getAttribute("role") || "").toLowerCase();
                    const style = window.getComputedStyle(el);
                    const onclick = el.onclick !== null || el.hasAttribute("onclick");
                    const hasBg = style.backgroundColor
                        && style.backgroundColor !== "rgba(0, 0, 0, 0)"
                        && style.backgroundColor !== "transparent";
                    const hasBorder = style.borderWidth && style.borderWidth !== "0px";
                    const semanticParent = el.closest(
                        "button, a, label, [role='button'], [role='radio'], [role='checkbox'], [role='option']"
                    );
                    return tag === "button"
                        || tag === "a"
                        || tag === "input"
                        || tag === "label"
                        || role === "button"
                        || role === "radio"
                        || role === "checkbox"
                        || role === "option"
                        || style.cursor === "pointer"
                        || onclick
                        || hasBg
                        || hasBorder
                        || Boolean(semanticParent);
                };

                const seen = new Set();
                const results = [];
                for (const el of document.querySelectorAll(selectors.join(","))) {
                    if (seen.has(el) || !isVisible(el) || !isClickable(el) || !hasUsefulText(el)) {
                        continue;
                    }
                    seen.add(el);
                    results.push(el);
                    if (results.length >= 80) {
                        break;
                    }
                }
                return results;
                """
            )
            if isinstance(candidates, list):
                return candidates
        except WebDriverException:
            pass
        return []

    def _try_direct_answer_recovery(self) -> bool:
        """
        Recover from screens where normal question detection failed but clickable
        answer buttons are visible.
        """
        candidates = self._get_clickable_text_candidates()
        if not candidates:
            return False

        predefined_answer, is_strict = self._find_predefined_answer()
        if predefined_answer:
            choice = self._find_element_by_text(candidates, predefined_answer)
            if choice:
                log(f"Recovered screen via direct predefined click: {predefined_answer}", self.profile_id)
                return safe_click(self.driver, choice)
            if is_strict:
                self._mark_required_predefined_missing(predefined_answer, candidates, "direct")
                return False

        choice = self._find_unknown_question_fallback_option(candidates)
        if not choice:
            return False

        try:
            label_text = (self._get_element_match_text(choice) or "(no text)")[:60]
        except WebDriverException:
            label_text = "(unknown)"
        log(f"Recovered screen via direct fallback click: {label_text}", self.profile_id)
        return safe_click(self.driver, choice)

    def _find_promotional_opt_out_button(self) -> WebElement | None:
        """
        Detect promo-style yes/no offer pages and prefer the explicit "No" choice.
        """
        page_text = get_page_text(self.driver)
        if not page_text:
            return None

        if not any(hint in page_text for hint in PROMOTIONAL_OPT_OUT_HINTS):
            return None

        candidates = self._get_clickable_text_candidates()
        if len(candidates) < 2:
            return None

        yes_choice = self._find_element_by_text(candidates, "yes")
        no_choice = self._find_element_by_text(candidates, "no")
        if yes_choice is not None and no_choice is not None:
            return no_choice
        return None

    def _find_terminal_claim_button(self) -> WebElement | None:
        """Find the final reward/claim CTA shown after the survey is done."""
        try:
            direct_match = self.driver.execute_script(
                """
                const keywords = arguments[0];
                const selectors = [
                    "a",
                    "button",
                    "input[type='button']",
                    "input[type='submit']",
                    "[role='button']",
                    "div",
                    "span"
                ];

                const normalize = (value) => (value || "").toLowerCase().replace(/[^a-z0-9$ ]+/g, " ").replace(/\s+/g, " ").trim();
                const isVisible = (el) => {
                    if (!el) return false;
                    const rect = el.getBoundingClientRect();
                    const style = window.getComputedStyle(el);
                    return rect.width > 20
                        && rect.height > 16
                        && style.display !== "none"
                        && style.visibility !== "hidden"
                        && style.opacity !== "0";
                };

                const getText = (el) => normalize(
                    el.innerText
                    || el.textContent
                    || el.value
                    || el.getAttribute("aria-label")
                    || el.getAttribute("title")
                    || ""
                );

                const clickableAncestor = (el) => {
                    if (!el) return null;
                    return el.closest("a, button, input[type='button'], input[type='submit'], [role='button'], label");
                };

                let best = null;
                let bestScore = -1;
                for (const el of document.querySelectorAll(selectors.join(","))) {
                    if (!isVisible(el)) continue;

                    const text = getText(el);
                    if (!text) continue;
                    if (!keywords.some((keyword) => text.includes(keyword))) continue;

                    const target = clickableAncestor(el) || el;
                    if (!isVisible(target)) continue;

                    let score = 0;
                    if (text.includes("claim")) score += 100;
                    if (text.includes("claim $")) score += 40;
                    if (text.includes("prize") || text.includes("reward")) score += 20;
                    if (target.tagName === "A" || target.tagName === "BUTTON") score += 15;
                    if ((target.getAttribute("role") || "").toLowerCase() === "button") score += 10;
                    if (target === el) score += 5;

                    const rect = target.getBoundingClientRect();
                    if (rect.top >= 0 && rect.top < window.innerHeight * 0.75) score += 5;

                    if (score > bestScore) {
                        best = target;
                        bestScore = score;
                    }
                }
                return best;
                """,
                list(TERMINAL_CLAIM_BUTTON_KEYWORDS),
            )
            if direct_match is not None:
                return direct_match
        except WebDriverException:
            pass

        best_match: WebElement | None = None
        best_score = -1

        for element in self._get_clickable_text_candidates():
            try:
                button_text = self._get_element_match_text(element)
            except WebDriverException:
                continue

            if not button_text:
                continue

            score = 0
            normalized = normalize_text(button_text)
            if "claim" in normalized.split():
                score += 3
            if any(keyword in normalized for keyword in TERMINAL_CLAIM_BUTTON_KEYWORDS):
                score += 1
            if "prize" in normalized or "reward" in normalized:
                score += 1

            if score > best_score:
                best_match = element
                best_score = score

        if best_score > 0:
            return best_match
        return None

    def _detect_terminal_claim_screen(self) -> tuple[WebElement | None, str]:
        """
        Detect the final reward screen that appears after survey completion.
        Returns the claim button plus its visible text when matched.
        """
        page_text = get_page_text(self.driver)
        if not page_text:
            return None, ""

        hint_count = sum(1 for hint in TERMINAL_CLAIM_PAGE_HINTS if hint in page_text)
        if hint_count < 2 and not (
            "claim your" in page_text and ("prize" in page_text or "reward" in page_text)
        ):
            return None, ""

        button = self._find_terminal_claim_button()
        if not button:
            log("Terminal claim hints found but no claim button was resolved", self.profile_id)
            return None, ""

        try:
            button_text = self._get_element_match_text(button)
        except WebDriverException:
            button_text = ""

        normalized_button_text = normalize_text(button_text)
        if not any(keyword in normalized_button_text for keyword in TERMINAL_CLAIM_BUTTON_KEYWORDS):
            return None, ""

        return button, normalized_button_text

    def _handle_terminal_claim_screen(self, question_num: int) -> bool:
        """
        Click the final claim CTA once and stop without letting the worker get stuck.
        """
        button, button_text = self._detect_terminal_claim_screen()
        if not button:
            return False

        old_handles: set[str] = set()
        try:
            old_handles = set(self.driver.window_handles)
        except WebDriverException:
            pass

        clicked = safe_click(self.driver, button)
        if clicked:
            log(f"Clicked terminal claim button: {button_text or '(claim)'}", self.profile_id)
            time.sleep(min(self.wait_after_click, 2.0))
        else:
            log(
                f"Terminal claim screen detected but click failed: {button_text or '(claim)'}",
                self.profile_id,
            )

        try:
            current_handles = self.driver.window_handles
            if (
                self._survey_tab_handle
                and self._survey_tab_handle in current_handles
                and self.driver.current_window_handle != self._survey_tab_handle
            ):
                self.driver.switch_to.window(self._survey_tab_handle)
                if len(current_handles) > len(old_handles):
                    log("Returned focus to locked survey tab after claim click", self.profile_id)
        except WebDriverException:
            pass

        self.result.questions_answered = max(question_num - 1, 0)
        if clicked:
            self._update_result(
                SurveyState.COMPLETED,
                "Final claim button clicked; stopped on completion screen",
            )
        else:
            self._update_result(
                SurveyState.COMPLETED,
                "Final claim screen detected; claim click failed but survey was not marked stuck",
            )
        self._screenshot("completed")
        return True

    def _find_continue_button(self) -> WebElement | None:
        """Find the Continue/Next/Submit button on the page."""
        # CSS selectors first
        for css in CONTINUE_BUTTON_CSS:
            elements = get_visible_elements(self.driver, By.CSS_SELECTOR, css)
            if elements:
                return elements[0]

        # XPath text-based selectors
        for xpath in CONTINUE_BUTTON_XPATH:
            elements = get_visible_elements(self.driver, By.XPATH, xpath)
            if elements:
                return elements[0]

        return None

    def _click_continue(self) -> bool:
        """Find and click the Continue/Next button. Returns True if clicked."""
        button = self._find_continue_button()
        if button:
            if safe_click(self.driver, button):
                log("Clicked Continue/Next button", self.profile_id)
                return True
            else:
                log("Continue button found but click failed", self.profile_id)
        return False

    def _wait_for_page_change(self, old_url: str, old_source_hash: int) -> None:
        """Wait for the page to change after clicking Continue."""
        deadline = time.time() + self.element_timeout
        while time.time() < deadline:
            try:
                new_url = self.driver.current_url
                if new_url != old_url:
                    time.sleep(1)  # small settle time
                    return
                new_hash = hash(self.driver.page_source)
                if new_hash != old_source_hash:
                    time.sleep(1)
                    return
            except WebDriverException:
                pass
            time.sleep(0.5)
        # Fallback: just wait the configured time
        time.sleep(self.wait_after_click)

    def _get_page_snapshot(self) -> tuple[str, int]:
        """Return (current_url, page_source_hash)."""
        try:
            url = self.driver.current_url
        except WebDriverException:
            url = ""
        try:
            source_hash = hash(self.driver.page_source)
        except WebDriverException:
            source_hash = 0
        return url, source_hash

    def _page_changed_since(self, old_url: str, old_source_hash: int) -> bool:
        try:
            new_url = self.driver.current_url
        except WebDriverException:
            new_url = old_url
        try:
            new_hash = hash(self.driver.page_source)
        except WebDriverException:
            new_hash = old_source_hash
        return (new_url != old_url) or (new_hash != old_source_hash)

    def _extract_option_texts(self, elements: list[WebElement]) -> list[str]:
        option_texts: list[str] = []
        seen: set[str] = set()
        for element in elements[:20]:
            try:
                text = self._get_element_match_text(element)
            except WebDriverException:
                text = ""
            normalized = normalize_text(text)
            if normalized and normalized not in seen:
                seen.add(normalized)
                option_texts.append(text or normalized)
        return option_texts

    def _build_question_context(
        self,
        q_type: QuestionType,
        elements: list[WebElement],
    ) -> dict[str, Any]:
        question_text = self._get_current_question_text()[:600]
        option_texts = self._extract_option_texts(elements)
        page_signature = ""
        if self.learning_store:
            page_signature = self.learning_store.build_page_signature(question_text, option_texts)
        return {
            "question_text": question_text,
            "option_texts": option_texts,
            "page_signature": page_signature,
            "question_type": q_type.value,
        }

    def _get_learned_answer(self) -> str | None:
        if not self.learning_store or not self._current_question_context:
            return None
        learned_answer = self.learning_store.get_learned_answer(
            question_text=self._current_question_context["question_text"],
            option_texts=self._current_question_context["option_texts"],
            page_signature=self._current_question_context["page_signature"],
        )
        if learned_answer:
            log(f"Using learned answer from memory: {learned_answer}", self.profile_id)
        return learned_answer

    def _record_pending_answer(
        self,
        element: WebElement | None,
        answer_text: str,
        source: str,
    ) -> None:
        if not self._current_question_context:
            return

        resolved_text = normalize_text(answer_text)
        if element is not None:
            try:
                element_text = self._get_element_match_text(element)
            except WebDriverException:
                element_text = ""
            if element_text:
                resolved_text = element_text

        if not resolved_text:
            return

        self._pending_answer_record = {
            "question_text": self._current_question_context["question_text"],
            "option_texts": self._current_question_context["option_texts"],
            "page_signature": self._current_question_context["page_signature"],
            "answer_text": resolved_text,
            "source": source,
        }

    def _commit_pending_answer_if_progressed(self, progressed: bool) -> None:
        if not progressed or not self._pending_answer_record:
            return
        self._session_answer_trail.append(dict(self._pending_answer_record))
        self._pending_answer_record = None

    def _finalize_learning(self, state: SurveyState, message: str) -> None:
        if self._learning_finalized or not self.learning_store:
            return

        if state == SurveyState.COMPLETED:
            for record in self._session_answer_trail:
                self.learning_store.remember_success(
                    question_text=record["question_text"],
                    answer_text=record["answer_text"],
                    option_texts=record["option_texts"],
                    page_signature=record["page_signature"],
                    source=record["source"],
                )
        elif state in {SurveyState.DISQUALIFIED, SurveyState.STUCK, SurveyState.FAILED}:
            failure_record = self._pending_answer_record or (
                self._session_answer_trail[-1] if self._session_answer_trail else None
            )
            if failure_record:
                self.learning_store.remember_failure(
                    question_text=failure_record["question_text"],
                    answer_text=failure_record["answer_text"],
                    option_texts=failure_record["option_texts"],
                    page_signature=failure_record["page_signature"],
                    reason=message,
                    source=failure_record["source"],
                )
            elif self._current_question_context:
                self.learning_store.note_page_issue(
                    question_text=self._current_question_context["question_text"],
                    option_texts=self._current_question_context["option_texts"],
                    page_signature=self._current_question_context["page_signature"],
                    reason=message,
                )

        self._learning_finalized = True

    # ---- answer logic ---------------------------------------------------

    def _get_current_question_text(self) -> str:
        """Extract the question text from the page."""
        page_text = get_page_text(self.driver)
        # Return normalized question text
        return normalize_text(page_text)

    def _find_predefined_answer(self) -> tuple[str | None, bool]:
        """
        Try to find a matching predefined answer for the current question.
        Returns (answer_text, is_strict) if found.
        """
        page_text = self._get_current_question_text()
        
        for question_pattern, answer in self.predefined_answers.items():
            # Check if question pattern is contained in page text
            if question_pattern in page_text:
                is_strict = question_pattern in self.strict_predefined_questions
                strict_note = " [strict]" if is_strict else ""
                log(f"Found predefined answer for question: {answer}{strict_note}", self.profile_id)
                return answer, is_strict
        
        return None, False

    def _mark_required_predefined_missing(
        self,
        answer: str,
        elements: list[WebElement],
        question_kind: str,
    ) -> None:
        option_labels: list[str] = []
        for element in elements[:8]:
            try:
                label = self._get_element_match_text(element)
            except WebDriverException:
                label = ""
            if label:
                option_labels.append(label[:60])

        options_text = "; ".join(option_labels) if option_labels else "no visible options captured"
        self._required_predefined_answer_missing = True
        self._required_predefined_message = (
            f"Required {question_kind} answer '{answer}' not found in visible options: {options_text}"
        )
        log(self._required_predefined_message, self.profile_id)

    def _get_element_match_text(self, element: WebElement) -> str:
        """Extract the most useful visible text for matching an option."""
        try:
            direct_text = normalize_text(
                " ".join(
                    part
                    for part in (
                        element.text or "",
                        element.get_attribute("aria-label") or "",
                        element.get_attribute("title") or "",
                        element.get_attribute("placeholder") or "",
                    )
                    if part
                )
            )
            if direct_text:
                return direct_text
        except WebDriverException:
            pass

        try:
            related_text = self.driver.execute_script(
                """
                const el = arguments[0];
                if (!el) return '';

                const pieces = [];
                const add = (value) => {
                    const text = (value || '').trim();
                    if (text) pieces.push(text);
                };

                add(el.innerText || el.textContent || '');
                add(el.value || '');
                add(el.getAttribute('aria-label') || '');
                add(el.getAttribute('title') || '');
                add(el.getAttribute('placeholder') || '');

                const labelledBy = (el.getAttribute('aria-labelledby') || '').trim();
                if (labelledBy) {
                    for (const id of labelledBy.split(/\s+/)) {
                        const labelledEl = document.getElementById(id);
                        if (labelledEl) {
                            add(labelledEl.innerText || labelledEl.textContent || '');
                        }
                    }
                }

                if (el.id) {
                    const labels = Array.from(document.getElementsByTagName('label'));
                    for (const label of labels) {
                        if (label.htmlFor === el.id) {
                            add(label.innerText || label.textContent || '');
                        }
                    }
                }

                const wrapperLabel = el.closest('label');
                if (wrapperLabel) {
                    add(wrapperLabel.innerText || wrapperLabel.textContent || '');
                }

                const semanticWrapper = el.closest(
                    "button, [role='radio'], [role='checkbox'], [role='option'], .answer-option, .survey-option, .choice, .option-item, .response-option"
                );
                if (semanticWrapper && semanticWrapper !== el) {
                    add(semanticWrapper.innerText || semanticWrapper.textContent || '');
                }

                if (el.previousElementSibling) {
                    add(el.previousElementSibling.innerText || el.previousElementSibling.textContent || '');
                }
                if (el.nextElementSibling) {
                    add(el.nextElementSibling.innerText || el.nextElementSibling.textContent || '');
                }

                return pieces.join(' | ');
                """,
                element,
            )
            normalized_related = normalize_text(related_text)
            if normalized_related:
                return normalized_related
        except WebDriverException:
            pass

        try:
            return normalize_text(element.get_attribute("value") or "")
        except WebDriverException:
            return ""

    def _find_element_by_text(self, elements: list[WebElement], target_text: str) -> WebElement | None:
        """Find an element whose text best matches target_text."""
        target_normalized = normalize_text(target_text).lower()
        if not target_normalized:
            return None

        exact_matches: list[WebElement] = []
        word_matches: list[WebElement] = []
        partial_matches: list[WebElement] = []
        target_words = target_normalized.split()

        for el in elements:
            try:
                el_text = self._get_element_match_text(el).lower()
                if not el_text:
                    continue

                if el_text == target_normalized:
                    exact_matches.append(el)
                    continue

                if re.search(rf"\b{re.escape(target_normalized)}\b", el_text):
                    word_matches.append(el)
                    continue

                if len(target_words) > 1 and all(word in el_text for word in target_words):
                    partial_matches.append(el)
                    continue

                if len(target_normalized) > 3 and target_normalized in el_text:
                    partial_matches.append(el)
            except WebDriverException:
                continue

        if exact_matches:
            return exact_matches[0]
        if word_matches:
            return word_matches[0]
        if partial_matches:
            return partial_matches[0]
        return None

    def _get_unknown_question_fallback_score(self, option_text: str) -> int:
        """
        Score opt-out style answers for questions that are not in data.txt.
        Higher score means a stronger match for safe negative answers.
        """
        normalized = normalize_text(option_text)
        if not normalized:
            return -1

        if "none of the above" in normalized or "none of these" in normalized:
            return 4
        if "none apply" in normalized:
            return 3
        if any(phrase in normalized for phrase in UNKNOWN_QUESTION_FALLBACK_PHRASES):
            return 2
        if any(phrase in normalized for phrase in UNKNOWN_QUESTION_NO_EXCLUSIONS):
            return -1
        if "no" in normalized.split():
            return 1
        return -1

    def _find_unknown_question_fallback_option(self, elements: list[WebElement]) -> WebElement | None:
        """
        For unmapped questions, prefer explicit opt-out answers like
        "No", "None", or "None of the above" before random selection.
        """
        best_match: WebElement | None = None
        best_score = -1

        for element in elements:
            try:
                option_text = self._get_element_match_text(element)
            except WebDriverException:
                continue

            score = self._get_unknown_question_fallback_score(option_text)
            if score > best_score:
                best_match = element
                best_score = score

        if best_score >= 0:
            return best_match
        return None

    def _get_preferred_random_option(self, elements: list[WebElement]) -> WebElement | None:
        """
        Select a random option, preferring negative/skip options like:
        "no", "never", "none of the above", "skip", etc.
        """
        if not elements:
            return None
        
        # Preferred keywords (prefer these)
        prefer_keywords = ["no", "never", "none", "skip", "not", "don't", "do not"]
        # Avoid keywords (avoid these)
        avoid_keywords = ["yes", "maybe", "somewhat"]
        
        preferred_options = []
        other_options = []
        
        for el in elements:
            try:
                combined = self._get_element_match_text(el).lower()
                
                # Check if it's in avoid list
                is_avoid = any(kw in combined for kw in avoid_keywords)
                # Check if it's in prefer list
                is_prefer = any(kw in combined for kw in prefer_keywords)
                
                if is_avoid:
                    other_options.append(el)
                elif is_prefer:
                    preferred_options.append(el)
                else:
                    other_options.append(el)
            except WebDriverException:
                other_options.append(el)
        
        # Prefer negative answers, but use all if not enough
        if preferred_options:
            return random.choice(preferred_options)
        return random.choice(other_options) if other_options else None

    def _answer_radio(self, radios: list[WebElement]) -> bool:
        """Select a radio button: predefined, fallback opt-out, else smart random."""
        if not radios:
            return False
        
        # Try predefined answer first
        predefined_answer, is_strict = self._find_predefined_answer()
        if predefined_answer:
            choice = self._find_element_by_text(radios, predefined_answer)
            if choice:
                log(f"Selecting predefined radio: {predefined_answer}", self.profile_id)
                if safe_click(self.driver, choice):
                    self._record_pending_answer(choice, predefined_answer, "predefined")
                    return True
                return False
            if is_strict:
                self._mark_required_predefined_missing(predefined_answer, radios, "radio")
                return False

        learned_answer = self._get_learned_answer()
        if learned_answer:
            choice = self._find_element_by_text(radios, learned_answer)
            if choice:
                log(f"Selecting learned radio: {learned_answer}", self.profile_id)
                if safe_click(self.driver, choice):
                    self._record_pending_answer(choice, learned_answer, "memory")
                    return True

        if not predefined_answer:
            choice = self._find_unknown_question_fallback_option(radios)
            if choice:
                try:
                    label_text = (self._get_element_match_text(choice) or "(no value)")[:50]
                except WebDriverException:
                    label_text = "(unknown)"
                log(f"Selecting fallback radio for unmapped question: {label_text}", self.profile_id)
                if safe_click(self.driver, choice):
                    self._record_pending_answer(choice, label_text, "fallback")
                    return True
                return False
        
        # Smart random selection
        choice = self._get_preferred_random_option(radios)
        if not choice:
            choice = random.choice(radios)
        
        try:
            label_text = (self._get_element_match_text(choice) or "(no value)")[:50]
        except WebDriverException:
            label_text = "(unknown)"
        log(f"Selecting radio: {label_text}", self.profile_id)
        if safe_click(self.driver, choice):
            self._record_pending_answer(choice, label_text, "random")
            return True
        return False

    def _answer_checkbox(self, checkboxes: list[WebElement]) -> bool:
        """Select 1 checkbox: predefined, fallback opt-out, else smart random."""
        if not checkboxes:
            return False
        
        # Try predefined answer first
        predefined_answer, is_strict = self._find_predefined_answer()
        if predefined_answer:
            choice = self._find_element_by_text(checkboxes, predefined_answer)
            if choice:
                log(f"Selecting predefined checkbox: {predefined_answer}", self.profile_id)
                if self._select_single_checkbox(checkboxes, choice):
                    self._record_pending_answer(choice, predefined_answer, "predefined")
                    return True
                return False
            if is_strict:
                self._mark_required_predefined_missing(predefined_answer, checkboxes, "checkbox")
                return False

        learned_answer = self._get_learned_answer()
        if learned_answer:
            choice = self._find_element_by_text(checkboxes, learned_answer)
            if choice:
                log(f"Selecting learned checkbox: {learned_answer}", self.profile_id)
                if self._select_single_checkbox(checkboxes, choice):
                    self._record_pending_answer(choice, learned_answer, "memory")
                    return True

        if not predefined_answer:
            choice = self._find_unknown_question_fallback_option(checkboxes)
            if choice:
                try:
                    label_text = (self._get_element_match_text(choice) or "(no value)")[:50]
                except WebDriverException:
                    label_text = "(unknown)"
                log(f"Selecting fallback checkbox for unmapped question: {label_text}", self.profile_id)
                if self._select_single_checkbox(checkboxes, choice):
                    self._record_pending_answer(choice, label_text, "fallback")
                    return True
                return False
        
        # Smart random selection (pick 1)
        choice = self._get_preferred_random_option(checkboxes)
        if not choice:
            choice = random.choice(checkboxes)
        
        try:
            label_text = (self._get_element_match_text(choice) or "(no value)")[:50]
        except WebDriverException:
            label_text = "(unknown)"
        log(f"Selecting checkbox: {label_text}", self.profile_id)
        if self._select_single_checkbox(checkboxes, choice):
            self._record_pending_answer(choice, label_text, "random")
            return True
        return False

    def _select_single_checkbox(
        self,
        checkboxes: list[WebElement],
        choice: WebElement,
    ) -> bool:
        """Ensure exactly one checkbox remains selected."""
        for checkbox in checkboxes:
            if checkbox == choice:
                continue
            try:
                if checkbox.is_selected():
                    self.driver.execute_script(
                        """
                        arguments[0].checked = false;
                        arguments[0].dispatchEvent(new Event('input', { bubbles: true }));
                        arguments[0].dispatchEvent(new Event('change', { bubbles: true }));
                        """,
                        checkbox,
                    )
            except WebDriverException:
                continue

        try:
            if choice.is_selected():
                return True
        except WebDriverException:
            pass

        if safe_click(self.driver, choice):
            return True

        try:
            self.driver.execute_script(
                """
                arguments[0].checked = true;
                arguments[0].dispatchEvent(new Event('input', { bubbles: true }));
                arguments[0].dispatchEvent(new Event('change', { bubbles: true }));
                """,
                choice,
            )
            return True
        except WebDriverException:
            return False

    def _answer_dropdown(self, selects: list[WebElement]) -> bool:
        """Select dropdown option: predefined, fallback opt-out, else smart random."""
        if not selects:
            return False
        select_el = selects[0]
        try:
            from selenium.webdriver.support.ui import Select
            sel = Select(select_el)
            options = sel.options
            # Filter out placeholder options (empty value, "Select...", etc.)
            valid_options = [
                opt for opt in options
                if opt.get_attribute("value")
                and opt.get_attribute("value").strip()
                and opt.get_attribute("value").strip().lower() not in ("", "0", "-1", "select", "choose")
            ]
            if not valid_options:
                valid_options = options[1:]  # skip first (usually placeholder)
            if valid_options:
                # Try predefined answer first
                predefined_answer, is_strict = self._find_predefined_answer()
                if predefined_answer:
                    choice = self._find_element_by_text(valid_options, predefined_answer)
                    if choice:
                        log(f"Selecting predefined dropdown: {predefined_answer}", self.profile_id)
                        sel.select_by_value(choice.get_attribute("value"))
                        self._record_pending_answer(choice, predefined_answer, "predefined")
                        return True
                    if is_strict:
                        self._mark_required_predefined_missing(predefined_answer, valid_options, "dropdown")
                        return False

                learned_answer = self._get_learned_answer()
                if learned_answer:
                    choice = self._find_element_by_text(valid_options, learned_answer)
                    if choice:
                        log(f"Selecting learned dropdown: {learned_answer}", self.profile_id)
                        sel.select_by_value(choice.get_attribute("value"))
                        self._record_pending_answer(choice, learned_answer, "memory")
                        return True

                if not predefined_answer:
                    choice = self._find_unknown_question_fallback_option(valid_options)
                    if choice:
                        log(f"Selecting fallback dropdown for unmapped question: {choice.text[:50]}", self.profile_id)
                        sel.select_by_value(choice.get_attribute("value"))
                        self._record_pending_answer(choice, choice.text[:50], "fallback")
                        return True
                
                # Smart random selection
                choice = self._get_preferred_random_option(valid_options)
                if not choice:
                    choice = random.choice(valid_options)
                
                log(f"Selecting dropdown: {choice.text[:50]}", self.profile_id)
                sel.select_by_value(choice.get_attribute("value"))
                self._record_pending_answer(choice, choice.text[:50], "random")
                return True
        except WebDriverException as exc:
            log(f"Dropdown selection failed: {exc}", self.profile_id)
        return False

    def _answer_button_options(self, buttons: list[WebElement]) -> bool:
        """Click button option: predefined, fallback opt-out, else smart random."""
        if not buttons:
            return False
        
        # Try predefined answer first
        predefined_answer, is_strict = self._find_predefined_answer()
        if predefined_answer:
            choice = self._find_element_by_text(buttons, predefined_answer)
            if choice:
                log(f"Clicking predefined answer button: {predefined_answer}", self.profile_id)
                if safe_click(self.driver, choice):
                    self._record_pending_answer(choice, predefined_answer, "predefined")
                    return True
                return False
            if is_strict:
                self._mark_required_predefined_missing(predefined_answer, buttons, "button")
                return False

        learned_answer = self._get_learned_answer()
        if learned_answer:
            choice = self._find_element_by_text(buttons, learned_answer)
            if choice:
                log(f"Clicking learned answer button: {learned_answer}", self.profile_id)
                if safe_click(self.driver, choice):
                    self._record_pending_answer(choice, learned_answer, "memory")
                    return True

        if not predefined_answer:
            choice = self._find_unknown_question_fallback_option(buttons)
            if choice:
                try:
                    label_text = (self._get_element_match_text(choice) or "(no text)")[:50]
                except WebDriverException:
                    label_text = "(unknown)"
                log(f"Clicking fallback answer for unmapped question: {label_text}", self.profile_id)
                if safe_click(self.driver, choice):
                    self._record_pending_answer(choice, label_text, "fallback")
                    return True
                return False
        
        # Smart random selection
        choice = self._get_preferred_random_option(buttons)
        if not choice:
            choice = random.choice(buttons)
        
        try:
            label_text = (choice.text or "(no text)").strip()[:50]
        except WebDriverException:
            label_text = "(unknown)"
        log(f"Clicking answer button: {label_text}", self.profile_id)
        if safe_click(self.driver, choice):
            self._record_pending_answer(choice, label_text, "random")
            return True
        return False

    def _answer_text_input(self, inputs: list[WebElement]) -> bool:
        """Type a generic answer into text inputs."""
        generic_answers = [
            "Good", "Yes", "No preference", "Neutral", "N/A",
            "Fine", "Okay", "Satisfactory", "Average", "None",
        ]
        for inp in inputs:
            answer = random.choice(generic_answers)
            try:
                inp.clear()
                inp.send_keys(answer)
                log(f"Typed '{answer}' into text input", self.profile_id)
            except WebDriverException as exc:
                log(f"Text input failed: {exc}", self.profile_id)
        return bool(inputs)

    def _answer_question(self, q_type: QuestionType, elements: list[WebElement]) -> bool:
        """Route to the appropriate answer method based on question type."""
        self._required_predefined_answer_missing = False
        self._required_predefined_message = ""
        self._current_question_context = self._build_question_context(q_type, elements)
        self._pending_answer_record = None
        if q_type == QuestionType.RADIO:
            return self._answer_radio(elements)
        if q_type == QuestionType.CHECKBOX:
            return self._answer_checkbox(elements)
        if q_type == QuestionType.DROPDOWN:
            return self._answer_dropdown(elements)
        if q_type == QuestionType.BUTTON_OPTIONS:
            return self._answer_button_options(elements)
        if q_type == QuestionType.TEXT_INPUT:
            return self._answer_text_input(elements)
        return False

    # ---- main entry point -----------------------------------------------

    def run(self) -> SurveyResult:
        """
        Execute the full survey answering flow for this browser.
        Loops through questions until completion, disqualification,
        max_questions, or failure.
        """
        log(f"Survey agent started (max_questions={self.max_questions})", self.profile_id)
        self._focus_correct_tab()

        # Log current URL after tab focus for debugging
        try:
            log(f"Current URL: {self.driver.current_url[:120]}", self.profile_id)
        except WebDriverException:
            pass

        stuck_count = 0

        for question_num in range(1, self.max_questions + 1):
            self._focus_correct_tab()

            # ---- Check for completion / disqualification ----
            try:
                if self._handle_terminal_claim_screen(question_num):
                    log("Final claim screen reached; worker stopped cleanly", self.profile_id)
                    return self.result

                promo_no_choice = self._find_promotional_opt_out_button()
                if promo_no_choice is not None:
                    old_url, old_source_hash = self._get_page_snapshot()
                    if safe_click(self.driver, promo_no_choice):
                        log("Clicked promotional opt-out answer: no", self.profile_id)
                        time.sleep(0.5)
                        page_changed = self._page_changed_since(old_url, old_source_hash)

                        if not page_changed:
                            if self._click_continue():
                                self._wait_for_page_change(old_url, old_source_hash)
                            else:
                                time.sleep(self.wait_after_click)
                        else:
                            time.sleep(1)
                        self.result.questions_answered = max(self.result.questions_answered, question_num)
                        continue

                if self._is_completed():
                    self.result.questions_answered = question_num - 1
                    self._update_result(SurveyState.COMPLETED, "Survey completed successfully")
                    self._screenshot("completed")
                    log(
                        f"Survey COMPLETED after {question_num - 1} questions",
                        self.profile_id,
                    )
                    return self.result

                if self._is_disqualified():
                    self.result.questions_answered = question_num - 1
                    self._update_result(SurveyState.DISQUALIFIED, "Screened out / disqualified")
                    self._screenshot("disqualified")
                    log("DISQUALIFIED from survey", self.profile_id)
                    return self.result
            except (WebDriverException, NoSuchWindowException) as exc:
                self._update_result(SurveyState.FAILED, f"Browser unreachable: {exc}")
                self._screenshot("failed")
                return self.result

            # ---- Detect question type ----
            try:
                q_type, elements = self.detector.detect(self.driver)
            except WebDriverException as exc:
                log(f"Detection error: {exc}", self.profile_id)
                q_type, elements = QuestionType.NONE, []

            if q_type == QuestionType.NONE:
                old_url, old_source_hash = self._get_page_snapshot()
                if self._try_direct_answer_recovery():
                    time.sleep(0.5)
                    page_changed = self._page_changed_since(old_url, old_source_hash)

                    if not page_changed:
                        if self._click_continue():
                            self._wait_for_page_change(old_url, old_source_hash)
                        else:
                            time.sleep(self.wait_after_click)
                    else:
                        time.sleep(1)
                    stuck_count = 0
                    continue

                # No question elements found — might be loading or stuck
                stuck_count += 1
                log(
                    f"No question found (stuck check {stuck_count}/{self.max_stuck_retries})",
                    self.profile_id,
                )

                # Re-focus the correct tab in case the browser switched tabs
                if stuck_count == 1 or stuck_count == 3:
                    self._focus_correct_tab()
                    try:
                        log(f"Re-checked tab URL: {self.driver.current_url[:120]}", self.profile_id)
                    except WebDriverException:
                        pass

                if stuck_count >= self.max_stuck_retries:
                    # Check one more time if it actually completed
                    if self._is_completed():
                        self.result.questions_answered = question_num - 1
                        self._update_result(SurveyState.COMPLETED, "Survey completed (detected after stall)")
                        self._screenshot("completed")
                        return self.result

                    self.result.questions_answered = question_num - 1
                    if self.learning_store:
                        current_question_text = self._get_current_question_text()[:600]
                        self.learning_store.note_page_issue(
                            question_text=current_question_text,
                            option_texts=[],
                            page_signature=self.learning_store.build_page_signature(current_question_text, []),
                            reason=f"Stuck after {self.max_stuck_retries} retries with no question elements",
                        )
                    self._update_result(
                        SurveyState.STUCK,
                        f"Stuck after {self.max_stuck_retries} retries — no question elements found",
                    )
                    self._screenshot("stuck")
                    return self.result

                # Wait and recheck
                time.sleep(self.stuck_wait)

                # Maybe there's just a Continue button with no question
                if self._click_continue():
                    log("Found Continue button with no question — clicked it", self.profile_id)
                    time.sleep(self.wait_after_click)
                    stuck_count = 0  # reset
                continue

            # ---- Question found — reset stuck counter ----
            stuck_count = 0
            log(f"Question #{question_num}: type={q_type.value}, options={len(elements)}", self.profile_id)

            # ---- Take page snapshot for change detection ----
            old_url, old_source_hash = self._get_page_snapshot()

            # ---- Answer the question ----
            answered = self._answer_question(q_type, elements)
            if not answered:
                if self._required_predefined_answer_missing:
                    self.result.questions_answered = question_num - 1
                    self._update_result(
                        SurveyState.STUCK,
                        self._required_predefined_message or "Required predefined answer could not be matched",
                    )
                    self._screenshot("required_answer_missing")
                    return self.result
                log("Failed to select an answer — trying Continue anyway", self.profile_id)

            # Small delay after selecting answer before clicking Continue
            time.sleep(0.5)

            # ---- Click Continue/Next ----
            # For button_options, clicking the button itself may advance
            # the page, so check if page already changed
            page_changed = self._page_changed_since(old_url, old_source_hash)

            if not page_changed:
                clicked = self._click_continue()
                if clicked:
                    self._wait_for_page_change(old_url, old_source_hash)
                else:
                    # No continue button — maybe answer click auto-advances
                    log("No Continue button found — waiting for auto-advance", self.profile_id)
                    time.sleep(self.wait_after_click)
            else:
                log("Page already changed after answer click", self.profile_id)
                time.sleep(1)  # settle time

            self._commit_pending_answer_if_progressed(self._page_changed_since(old_url, old_source_hash))
            self.result.questions_answered = question_num

        # ---- Max questions reached ----
        # Final completion check
        if self._handle_terminal_claim_screen(self.max_questions + 1):
            return self.result
        if self._is_completed():
            self._update_result(SurveyState.COMPLETED, f"Survey completed after {self.max_questions} questions")
            self._screenshot("completed")
        else:
            self._update_result(
                SurveyState.FAILED,
                f"Reached max_questions limit ({self.max_questions}) without completion",
            )
            self._screenshot("max_questions")

        return self.result


# ===================================================================
# SurveyAgent — orchestrator for all browsers
# ===================================================================

class SurveyAgent:
    """
    Connects to all active ADSPower profiles and runs a SurveyWorker
    thread for each one to answer surveys concurrently.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.adspower_config = config.get("adspower", {})
        self.survey_config = config.get("survey", {})
        self.client = AdsPowerClient(self.adspower_config)
        self.learning_store = SurveyLearningStore(
            str(self.survey_config.get("memory_file", "survey_memory.json"))
        )
        self.results: list[SurveyResult] = []
        self._results_lock = threading.Lock()
        self._profile_connect_lock = threading.Lock()
        self.discovery_retry_bonus: int = self.learning_store.get_discovery_retry_bonus()
        self.attach_retries: int = max(1, int(self.survey_config.get("attach_retries", 4)))
        self.attach_retry_delay: float = max(
            0.25,
            float(self.survey_config.get("attach_retry_delay_seconds", 1)),
        )
        self.profile_discovery_retries: int = max(
            1,
            int(self.survey_config.get("profile_discovery_retries", 3)) + self.discovery_retry_bonus,
        )
        self.profile_discovery_stable_rounds: int = max(
            1,
            int(self.survey_config.get("profile_discovery_stable_rounds", 1)),
        )
        self.profile_discovery_wait: float = max(
            0.25,
            float(self.survey_config.get("profile_discovery_wait_seconds", 0.75)),
        )
        self.thread_start_stagger: float = max(
            0.0,
            float(self.survey_config.get("thread_start_stagger_seconds", 0.0)),
        )
        
        # Load predefined answers
        data_file = self.survey_config.get("data_file", "data.txt")
        self.predefined_answers = load_predefined_answers(data_file)

    def _append_result(self, result: SurveyResult) -> None:
        with self._results_lock:
            self.results.append(result)

    def _collect_active_profiles(self) -> list[dict[str, Any]]:
        discovered: dict[str, dict[str, Any]] = {}
        stable_rounds = 0
        last_count = -1

        for attempt in range(1, self.profile_discovery_retries + 1):
            active = self.client.list_active_profiles()
            for profile_data in active:
                pid = str(profile_data.get("user_id", "")).strip()
                if pid:
                    discovered[pid] = profile_data

            log(
                f"Active profile discovery {attempt}/{self.profile_discovery_retries}: {len(discovered)} unique profile(s)",
            )

            current_count = len(discovered)
            if current_count == last_count:
                stable_rounds += 1
            else:
                stable_rounds = 0
            last_count = current_count

            if current_count > 0 and stable_rounds >= self.profile_discovery_stable_rounds:
                break

            if attempt < self.profile_discovery_retries:
                time.sleep(self.profile_discovery_wait)

        return list(discovered.values())

    def _resolve_profiles(self) -> list[dict[str, Any]]:
        explicit_ids = [
            str(pid).strip()
            for pid in self.adspower_config.get("profile_ids", [])
            if str(pid).strip()
        ]

        if explicit_ids:
            return [
                {"user_id": pid, "connect_mode": "start"}
                for pid in explicit_ids
            ]

        if bool(self.adspower_config.get("use_active_profiles", True)):
            active = self._collect_active_profiles()
            if not active:
                raise RuntimeError(
                    "No open ADSPower browsers found. "
                    "Open the desired browser profiles first, then run again."
                )
            profiles = []
            seen_profile_ids: set[str] = set()
            for profile_data in active:
                pid = str(profile_data.get("user_id", "")).strip()
                if not pid or pid in seen_profile_ids:
                    continue
                seen_profile_ids.add(pid)
                profiles.append(
                    {
                        "user_id": pid,
                        "connect_mode": "attach",
                        "browser_data": profile_data,
                    }
                )
            profiles.sort(
                key=lambda profile: self.learning_store.get_profile_priority(str(profile.get("user_id", ""))),
                reverse=True,
            )
            return profiles

        raise RuntimeError("No ADSPower profiles configured.")

    def _get_active_profile_data(self, profile_id: str) -> dict[str, Any] | None:
        try:
            active = self.client.list_active_profiles()
        except Exception as exc:
            log(f"Could not refresh active profile list: {exc}", profile_id)
            return None

        for profile_data in active:
            if str(profile_data.get("user_id", "")).strip() == profile_id:
                return profile_data
        return None

    def _merge_missing_active_profiles(self, profiles: list[dict[str, Any]]) -> list[dict[str, Any]]:
        explicit_ids = [
            str(pid).strip()
            for pid in self.adspower_config.get("profile_ids", [])
            if str(pid).strip()
        ]
        if explicit_ids:
            return list(profiles)

        if not bool(self.adspower_config.get("use_active_profiles", True)):
            return list(profiles)

        merged_profiles = list(profiles)
        known_ids = {
            str(profile.get("user_id", "")).strip()
            for profile in merged_profiles
            if str(profile.get("user_id", "")).strip()
        }

        refreshed_active = self._collect_active_profiles()
        added_ids: list[str] = []
        for profile_data in refreshed_active:
            pid = str(profile_data.get("user_id", "")).strip()
            if not pid or pid in known_ids:
                continue
            merged_profiles.append(
                {
                    "user_id": pid,
                    "connect_mode": "attach",
                    "browser_data": profile_data,
                }
            )
            known_ids.add(pid)
            added_ids.append(pid)

        if added_ids:
            log(f"Added missing active profile(s) after refresh: {', '.join(sorted(added_ids))}")

        return merged_profiles

    def _run_profile_batch(self, profiles: list[dict[str, Any]]) -> None:
        threads: list[threading.Thread] = []
        for profile in profiles:
            t = threading.Thread(
                target=self._worker_thread,
                args=(profile,),
                daemon=False,
            )
            threads.append(t)
            t.start()
            if self.thread_start_stagger:
                time.sleep(self.thread_start_stagger)

        for t in threads:
            t.join()

    def _launch_profile_threads(
        self,
        profiles: list[dict[str, Any]],
        launched_profile_ids: set[str],
        threads_by_profile_id: dict[str, threading.Thread],
    ) -> list[str]:
        started_ids: list[str] = []
        for profile in profiles:
            profile_id = str(profile.get("user_id", "")).strip()
            if not profile_id or profile_id in launched_profile_ids:
                continue

            thread = threading.Thread(
                target=self._worker_thread,
                args=(profile,),
                daemon=False,
            )
            launched_profile_ids.add(profile_id)
            threads_by_profile_id[profile_id] = thread
            started_ids.append(profile_id)
            thread.start()
            if self.thread_start_stagger:
                time.sleep(self.thread_start_stagger)
        return started_ids

    def _discover_late_profiles(self, launched_profile_ids: set[str]) -> list[dict[str, Any]]:
        explicit_ids = [
            str(pid).strip()
            for pid in self.adspower_config.get("profile_ids", [])
            if str(pid).strip()
        ]
        if explicit_ids or not bool(self.adspower_config.get("use_active_profiles", True)):
            return []

        late_profiles: list[dict[str, Any]] = []
        stable_rounds = 0
        last_seen_count = -1

        for attempt in range(1, self.profile_discovery_retries + 1):
            try:
                active = self.client.list_active_profiles()
            except Exception as exc:
                log(f"Late profile discovery failed on attempt {attempt}: {exc}")
                active = []

            active_ids: set[str] = set()
            new_profiles_this_round: list[dict[str, Any]] = []
            for profile_data in active:
                pid = str(profile_data.get("user_id", "")).strip()
                if not pid:
                    continue
                active_ids.add(pid)
                if pid in launched_profile_ids:
                    continue
                new_profiles_this_round.append(
                    {
                        "user_id": pid,
                        "connect_mode": "attach",
                        "browser_data": profile_data,
                    }
                )

            if new_profiles_this_round:
                late_profiles.extend(new_profiles_this_round)
                for profile in new_profiles_this_round:
                    launched_profile_ids.add(str(profile["user_id"]))
                log(
                    "Late profile discovery found new profile(s): "
                    + ", ".join(str(profile["user_id"]) for profile in new_profiles_this_round)
                )

            current_count = len(active_ids)
            if current_count == last_seen_count:
                stable_rounds += 1
            else:
                stable_rounds = 0
            last_seen_count = current_count

            if active_ids and stable_rounds >= self.profile_discovery_stable_rounds:
                break

            if attempt < self.profile_discovery_retries:
                time.sleep(self.profile_discovery_wait)

        # launched_profile_ids is only for de-dup during discovery; remove late profiles so they can be started.
        for profile in late_profiles:
            launched_profile_ids.discard(str(profile["user_id"]))

        return late_profiles

    def _connect_profile_driver(self, profile: dict[str, Any]) -> webdriver.Chrome:
        profile_id = str(profile["user_id"])
        connect_mode = str(profile.get("connect_mode", "attach")).strip().lower()
        browser_data = profile.get("browser_data")
        last_error: Exception | None = None
        effective_attach_retries = self.learning_store.get_profile_attach_retry_count(
            profile_id,
            self.attach_retries,
        )

        for attempt in range(1, effective_attach_retries + 1):
            try:
                if connect_mode == "start":
                    with self._profile_connect_lock:
                        driver, _ = self.client.start_profile(profile_id)
                    log(
                        f"Connected to profile on attempt {attempt}/{effective_attach_retries}",
                        profile_id,
                    )
                    return driver

                current_browser_data = browser_data
                if attempt > 1 or not current_browser_data:
                    current_browser_data = self._get_active_profile_data(profile_id)
                    if current_browser_data:
                        profile["browser_data"] = current_browser_data
                        browser_data = current_browser_data

                if not current_browser_data:
                    raise RuntimeError("Profile is not present in AdsPower local-active list")

                driver = self.client.attach_to_active_profile(current_browser_data)
                log(
                    f"Attached to active profile on attempt {attempt}/{effective_attach_retries}",
                    profile_id,
                )
                return driver
            except Exception as exc:
                last_error = exc
                action = "start" if connect_mode == "start" else "attach"
                log(
                    f"Could not {action} profile on attempt {attempt}/{effective_attach_retries}: {exc}",
                    profile_id,
                )
                if attempt < effective_attach_retries:
                    time.sleep(self.attach_retry_delay)

        raise RuntimeError(
            f"Could not connect to profile after {effective_attach_retries} attempts: {last_error}"
        )

    def _worker_thread(self, profile: dict[str, Any]) -> None:
        profile_id = str(profile["user_id"])
        log("Worker assigned to profile", profile_id)

        try:
            driver = self._connect_profile_driver(profile)
        except Exception as exc:
            log(f"Worker could not connect to profile: {exc}", profile_id)
            self.learning_store.note_profile_result(profile_id, "connect_failed", str(exc))
            error_result = SurveyResult(
                profile_id=profile_id,
                state=SurveyState.FAILED.value,
                message=f"Could not connect to profile: {exc}",
            )
            error_result.refresh_timestamp()
            self._append_result(error_result)
            return

        try:
            worker = SurveyWorker(
                driver=driver,
                profile_id=profile_id,
                config=self.survey_config,
                predefined_answers=self.predefined_answers,
                learning_store=self.learning_store,
            )
            result = worker.run()
            self.learning_store.note_profile_result(profile_id, result.state, result.message)
            self._append_result(result)
        except Exception as exc:
            log(f"Survey thread crashed: {exc}", profile_id)
            self.learning_store.note_profile_result(profile_id, "thread_crashed", str(exc))
            error_result = SurveyResult(
                profile_id=profile_id,
                state=SurveyState.FAILED.value,
                message=f"Thread crashed: {exc}",
            )
            error_result.refresh_timestamp()
            self._append_result(error_result)

    def run(self) -> list[SurveyResult]:
        """Run the survey agent across all browsers."""
        log("=" * 60)
        log("  ADSPower Survey Agent — Starting")
        log("=" * 60)

        profiles = self._resolve_profiles()
        if not profiles:
            log("No browser profiles available. Exiting.")
            return []

        log(f"Found {len(profiles)} browser profile(s) for survey filling.")
        profile_ids = ", ".join(str(profile["user_id"]) for profile in profiles)
        if profile_ids:
            log(f"Target profile IDs: {profile_ids}")
        expected_profile_ids = {
            str(profile.get("user_id", "")).strip()
            for profile in profiles
            if str(profile.get("user_id", "")).strip()
        }

        launched_profile_ids: set[str] = set()
        threads_by_profile_id: dict[str, threading.Thread] = {}
        self._launch_profile_threads(profiles, launched_profile_ids, threads_by_profile_id)

        late_profiles = self._discover_late_profiles(launched_profile_ids)
        if late_profiles:
            late_ids = ", ".join(str(profile["user_id"]) for profile in late_profiles)
            log(f"Launching late-discovered profile(s): {late_ids}")
            profiles.extend(late_profiles)
            expected_profile_ids.update(str(profile["user_id"]) for profile in late_profiles)
            self._launch_profile_threads(late_profiles, launched_profile_ids, threads_by_profile_id)

        for thread in threads_by_profile_id.values():
            thread.join()

        result_profile_ids = {result.profile_id for result in self.results}
        missing_ids = sorted(expected_profile_ids - result_profile_ids)
        if missing_ids:
            retry_profiles = [
                profile
                for profile in profiles
                if str(profile.get("user_id", "")).strip() in set(missing_ids)
            ]
            if retry_profiles:
                log(
                    "Launching recovery batch for profiles with no recorded result: "
                    + ", ".join(str(profile["user_id"]) for profile in retry_profiles)
                )
                recovery_launched_ids: set[str] = set()
                recovery_threads: dict[str, threading.Thread] = {}
                self._launch_profile_threads(retry_profiles, recovery_launched_ids, recovery_threads)
                for thread in recovery_threads.values():
                    thread.join()
                result_profile_ids = {result.profile_id for result in self.results}
                missing_ids = sorted(expected_profile_ids - result_profile_ids)

        for missing_id in missing_ids:
            log("No result was recorded for discovered profile after recovery passes", missing_id)
            self.learning_store.note_profile_result(
                missing_id,
                "no_result",
                "No result was recorded for this discovered profile after recovery passes.",
            )
            error_result = SurveyResult(
                profile_id=missing_id,
                state=SurveyState.FAILED.value,
                message="No result was recorded for this discovered profile after recovery passes.",
            )
            error_result.refresh_timestamp()
            self._append_result(error_result)

        # ---- Save log file ----
        log_dir = self.survey_config.get("log_directory", "logs")
        log_path = save_log_file(log_dir)
        log(f"Full log saved to: {log_path}")

        # ---- Print summary ----
        self._print_summary()

        return self.results

    def _print_summary(self) -> None:
        completed = [r for r in self.results if r.state == SurveyState.COMPLETED.value]
        disqualified = [r for r in self.results if r.state == SurveyState.DISQUALIFIED.value]
        stuck = [r for r in self.results if r.state == SurveyState.STUCK.value]
        failed = [r for r in self.results if r.state == SurveyState.FAILED.value]

        log("")
        log("=" * 60)
        log("  SURVEY AGENT SUMMARY")
        log("=" * 60)
        log(f"  Total profiles   : {len(self.results)}")
        log(f"  Completed        : {len(completed)}")
        log(f"  Disqualified     : {len(disqualified)}")
        log(f"  Stuck            : {len(stuck)}")
        log(f"  Failed           : {len(failed)}")
        log("-" * 60)

        for r in self.results:
            if r.state == SurveyState.COMPLETED.value:
                icon = "[OK]"
            elif r.state == SurveyState.DISQUALIFIED.value:
                icon = "[DQ]"
            else:
                icon = "[FAIL]"

            log(f"  {icon} [{r.profile_id}] {r.state.upper()} -- {r.questions_answered} questions -- {r.message}")
            if r.url:
                log(f"       URL: {r.url}")
            if r.screenshot_path:
                log(f"       Screenshot: {r.screenshot_path}")

        log("=" * 60)

        total_questions = sum(r.questions_answered for r in self.results)
        log(f"\nTotal questions answered across all browsers: {total_questions}")


# ===================================================================
# Config loader & CLI
# ===================================================================

def load_survey_config(config_path: Path) -> dict[str, Any]:
    if not config_path.exists():
        raise FileNotFoundError(
            f"Survey config not found: {config_path}. "
            "Create survey_config.json first."
        )
    return json.loads(config_path.read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Automatically answer surveys across multiple ADSPower browsers."
    )
    parser.add_argument(
        "--config",
        default="survey_config.json",
        help="Path to survey config JSON file (default: survey_config.json)",
    )
    args = parser.parse_args()

    config_path = Path(args.config).expanduser().resolve()
    config = load_survey_config(config_path)

    agent = SurveyAgent(config)
    agent.run()


if __name__ == "__main__":
    main()
