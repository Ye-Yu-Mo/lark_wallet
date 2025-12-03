import os
import json
import pytest # Using pytest framework for testing
from lib.smart_categorizer import SmartCategorizer

# Define paths
# Assuming corrections.json is in the same directory as smart_categorizer.py
# So its path relative to this test file would be lib/corrections.json
# Need to make sure the path in SmartCategorizer.CORRECTIONS_FILE is correct.
# It is os.path.join(os.path.dirname(__file__), 'corrections.json') inside lib/smart_categorizer.py
# So if this test is run from root, then lib/corrections.json will be created.

# Let's adjust the test to clean up the actual corrections.json path used by SmartCategorizer
# For tests, it's better to ensure a clean state
TEST_CORRECTIONS_FILE = os.path.join(os.path.dirname(SmartCategorizer.CORRECTIONS_FILE), 'corrections.json')


@pytest.fixture(autouse=True)
def setup_and_teardown_corrections_file():
    """Fixture to ensure corrections.json is clean before and after each test."""
    # Setup: Clean or create empty corrections.json
    if os.path.exists(TEST_CORRECTIONS_FILE):
        os.remove(TEST_CORRECTIONS_FILE)
    with open(TEST_CORRECTIONS_FILE, 'w', encoding='utf-8') as f:
        json.dump({}, f)
    SmartCategorizer._load_corrections() # Reload empty corrections for the class

    yield # This is where the test runs

    # Teardown: Clean up corrections.json after test
    if os.path.exists(TEST_CORRECTIONS_FILE):
        os.remove(TEST_CORRECTIONS_FILE)
    SmartCategorizer._load_corrections() # Reset corrections for potential subsequent tests if not using autouse=True

def test_initial_categorization_without_correction():
    """测试在没有修正记录时，按原有规则进行分类"""
    # 模拟一个会按原有规则分类的交易
    counterparty = "星巴克"
    category = "消费" # Initial category from raw data (could be anything, not used if counterparty matches rule)
    source_type = "wechat"
    is_income = False
    
    # 预期结果来自KEYWORD_RULES
    expected_category = "餐饮" 
    actual_category = SmartCategorizer.categorize(source_type, category, counterparty, is_income)
    assert actual_category == expected_category, \
        f"Expected initial category '{expected_category}', but got '{actual_category}'"

def test_add_and_apply_correction():
    """测试添加修正后，新分类能被正确应用"""
    counterparty_to_correct = "Moonbucks Cafe"
    initial_category_from_rules = "其他" # Assume rules would classify it as '其他'
    new_correct_category = "咖啡" # A more specific category
    source_type = "alipay"
    is_income = False

    # Add correction
    SmartCategorizer.add_correction(counterparty_to_correct, new_correct_category)

    # Verify corrections.json is updated
    with open(TEST_CORRECTIONS_FILE, 'r', encoding='utf-8') as f:
        corrections_data = json.load(f)
        assert SmartCategorizer._clean_counterparty(counterparty_to_correct) in corrections_data
        assert corrections_data[SmartCategorizer._clean_counterparty(counterparty_to_correct)] == new_correct_category

    # Recategorize, should use the corrected category
    corrected_category = SmartCategorizer.categorize(source_type, "消费", counterparty_to_correct, is_income)
    assert corrected_category == new_correct_category, \
        f"Expected corrected category '{new_correct_category}', but got '{corrected_category}'"

def test_correction_priority():
    """测试修正记录优先于原有规则"""
    counterparty_with_rule = "海底捞" # In KEYWORD_RULES as '餐饮'
    new_correction = "娱乐" # User wants to classify it as '娱乐' for some reason
    source_type = "wechat"
    is_income = False

    # Add correction, overriding existing rule
    SmartCategorizer.add_correction(counterparty_with_rule, new_correction)

    # Verify categorization result should be the corrected category
    actual_category = SmartCategorizer.categorize(source_type, "消费", counterparty_with_rule, is_income)
    assert actual_category == new_correction, \
        f"Correction for '{counterparty_with_rule}' not prioritized. Expected '{new_correction}', got '{actual_category}'"

def test_clean_counterparty_logic():
    """测试交易对方清理逻辑"""
    assert SmartCategorizer._clean_counterparty("Test (123)") == "Test"
    assert SmartCategorizer._clean_counterparty("Test（123）") == "Test"
    assert SmartCategorizer._clean_counterparty("Test*123") == "Test123"
    assert SmartCategorizer._clean_counterparty(" Test ") == "Test"
    assert SmartCategorizer._clean_counterparty(" ") == ""
    assert SmartCategorizer._clean_counterparty(None) == "" # Should handle None input
    assert SmartCategorizer._clean_counterparty("支付宝（交通）") == "支付宝"

def test_empty_corrections_file():
    """测试空的 corrections.json 文件"""
    # Ensure corrections.json is truly empty (not just {})
    with open(TEST_CORRECTIONS_FILE, 'w', encoding='utf-8') as f:
        f.write("")
    SmartCategorizer._load_corrections()
    assert SmartCategorizer._corrections == {}

    # Test categorization still works with empty corrections
    counterparty = "星巴克"
    category = "消费"
    source_type = "wechat"
    is_income = False
    expected_category = "餐饮"
    actual_category = SmartCategorizer.categorize(source_type, category, counterparty, is_income)
    assert actual_category == expected_category
