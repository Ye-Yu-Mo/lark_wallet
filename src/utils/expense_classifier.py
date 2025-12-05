"""
支出分类器
使用朴素贝叶斯算法自动预测支出目的和细类
"""
from typing import Dict, List, Tuple, Optional
from collections import defaultdict, Counter
import re
import math
from loguru import logger


class ExpenseClassifier:
    """
    支出分类器

    使用朴素贝叶斯算法基于历史数据预测支出目的和细类
    """

    def __init__(self):
        """初始化分类器"""
        # 用于支出目的分类
        self.purpose_word_counts = defaultdict(lambda: defaultdict(int))  # {目的: {词: 次数}}
        self.purpose_total_counts = defaultdict(int)  # {目的: 总词数}
        self.purpose_doc_counts = defaultdict(int)  # {目的: 文档数}
        self.purpose_vocab = set()  # 词汇表

        # 用于细类分类
        self.subcat_word_counts = defaultdict(lambda: defaultdict(int))  # {细类: {词: 次数}}
        self.subcat_total_counts = defaultdict(int)  # {细类: 总词数}
        self.subcat_doc_counts = defaultdict(int)  # {细类: 文档数}
        self.subcat_vocab = set()  # 词汇表

        # 类别与主分类的关系 (用于提高准确率)
        self.category_purpose_map = defaultdict(Counter)  # {分类: {目的: 次数}}
        self.category_subcat_map = defaultdict(Counter)  # {分类: {细类: 次数}}

        self.total_docs = 0
        self.is_trained = False

    def tokenize(self, text: str) -> List[str]:
        """
        分词函数

        :param text: 文本
        :return: 词列表
        """
        if not text:
            return []

        # 简单的中文分词：按字符切分 + 提取连续的英文/数字
        tokens = []

        # 提取连续的中文字符（单字）
        chinese_chars = re.findall(r'[\u4e00-\u9fff]', text)
        tokens.extend(chinese_chars)

        # 提取连续的英文/数字词
        words = re.findall(r'[a-zA-Z0-9]+', text.lower())
        tokens.extend(words)

        return tokens

    def train(self, records: List[Dict]):
        """
        训练分类器

        :param records: 历史记录列表，每条记录包含 {备注, 分类, 支出目的, 细类, 收支}
        """
        logger.info(f"开始训练分类器，共 {len(records)} 条记录...")

        # 只使用支出记录进行训练
        expense_records = [r for r in records if r.get('收支') == '支出']

        if not expense_records:
            logger.warning("没有支出记录，无法训练分类器")
            return

        self.total_docs = len(expense_records)

        for record in expense_records:
            note = str(record.get('备注', '')).strip()
            category = str(record.get('分类', '')).strip()
            purpose = str(record.get('支出目的', '')).strip()
            subcat = str(record.get('细类', '')).strip()

            if not note:
                continue

            tokens = self.tokenize(note)

            # 训练支出目的分类器
            if purpose:
                self.purpose_doc_counts[purpose] += 1
                for token in tokens:
                    self.purpose_word_counts[purpose][token] += 1
                    self.purpose_total_counts[purpose] += 1
                    self.purpose_vocab.add(token)

                # 记录分类与目的的关系
                if category:
                    self.category_purpose_map[category][purpose] += 1

            # 训练细类分类器
            if subcat:
                self.subcat_doc_counts[subcat] += 1
                for token in tokens:
                    self.subcat_word_counts[subcat][token] += 1
                    self.subcat_total_counts[subcat] += 1
                    self.subcat_vocab.add(token)

                # 记录分类与细类的关系
                if category:
                    self.category_subcat_map[category][subcat] += 1

        self.is_trained = True
        logger.info(f"训练完成: 支出目的 {len(self.purpose_doc_counts)} 类, "
                   f"细类 {len(self.subcat_doc_counts)} 类, "
                   f"词汇量 {len(self.purpose_vocab | self.subcat_vocab)}")

    def predict_purpose(self, note: str, category: Optional[str] = None, top_k: int = 1) -> List[Tuple[str, float]]:
        """
        预测支出目的

        :param note: 备注文本
        :param category: 主分类（可选，用于提高准确率）
        :param top_k: 返回前k个最可能的类别
        :return: [(目的, 概率)] 列表
        """
        if not self.is_trained or not self.purpose_doc_counts:
            return []

        tokens = self.tokenize(note)
        if not tokens:
            return []

        scores = {}
        vocab_size = len(self.purpose_vocab)

        for purpose in self.purpose_doc_counts:
            # 计算先验概率 P(目的)
            prior = math.log(self.purpose_doc_counts[purpose] / self.total_docs)

            # 如果提供了主分类，调整先验概率
            if category and category in self.category_purpose_map:
                category_total = sum(self.category_purpose_map[category].values())
                category_prior = self.category_purpose_map[category].get(purpose, 0) / category_total if category_total > 0 else 0
                if category_prior > 0:
                    prior = math.log(category_prior * 0.7 + self.purpose_doc_counts[purpose] / self.total_docs * 0.3)

            # 计算似然概率 P(词|目的)
            likelihood = 0
            for token in tokens:
                # 拉普拉斯平滑
                word_count = self.purpose_word_counts[purpose].get(token, 0)
                likelihood += math.log((word_count + 1) / (self.purpose_total_counts[purpose] + vocab_size))

            scores[purpose] = prior + likelihood

        # 排序并归一化为概率
        sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_k]

        # 转换为概率（softmax）
        max_score = sorted_scores[0][1]
        exp_scores = [(p, math.exp(s - max_score)) for p, s in sorted_scores]
        total_exp = sum(e for _, e in exp_scores)
        probabilities = [(p, e / total_exp) for p, e in exp_scores]

        return probabilities

    def predict_subcat(self, note: str, category: Optional[str] = None, top_k: int = 1) -> List[Tuple[str, float]]:
        """
        预测细类

        :param note: 备注文本
        :param category: 主分类（可选，用于提高准确率）
        :param top_k: 返回前k个最可能的类别
        :return: [(细类, 概率)] 列表
        """
        if not self.is_trained or not self.subcat_doc_counts:
            return []

        tokens = self.tokenize(note)
        if not tokens:
            return []

        scores = {}
        vocab_size = len(self.subcat_vocab)

        for subcat in self.subcat_doc_counts:
            # 计算先验概率 P(细类)
            prior = math.log(self.subcat_doc_counts[subcat] / self.total_docs)

            # 如果提供了主分类，调整先验概率
            if category and category in self.category_subcat_map:
                category_total = sum(self.category_subcat_map[category].values())
                category_prior = self.category_subcat_map[category].get(subcat, 0) / category_total if category_total > 0 else 0
                if category_prior > 0:
                    prior = math.log(category_prior * 0.7 + self.subcat_doc_counts[subcat] / self.total_docs * 0.3)

            # 计算似然概率 P(词|细类)
            likelihood = 0
            for token in tokens:
                # 拉普拉斯平滑
                word_count = self.subcat_word_counts[subcat].get(token, 0)
                likelihood += math.log((word_count + 1) / (self.subcat_total_counts[subcat] + vocab_size))

            scores[subcat] = prior + likelihood

        # 排序并归一化为概率
        sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_k]

        # 转换为概率（softmax）
        max_score = sorted_scores[0][1]
        exp_scores = [(s, math.exp(sc - max_score)) for s, sc in sorted_scores]
        total_exp = sum(e for _, e in exp_scores)
        probabilities = [(s, e / total_exp) for s, e in exp_scores]

        return probabilities

    def predict(self, note: str, category: Optional[str] = None,
                confidence_threshold: float = 0.3) -> Dict[str, Optional[str]]:
        """
        预测支出目的和细类

        :param note: 备注文本
        :param category: 主分类（可选）
        :param confidence_threshold: 置信度阈值，低于此值则返回None
        :return: {'purpose': 支出目的, 'subcat': 细类, 'purpose_confidence': 置信度, 'subcat_confidence': 置信度}
        """
        result = {
            'purpose': None,
            'subcat': None,
            'purpose_confidence': 0.0,
            'subcat_confidence': 0.0
        }

        # 预测支出目的
        purpose_predictions = self.predict_purpose(note, category, top_k=1)
        if purpose_predictions:
            purpose, confidence = purpose_predictions[0]
            if confidence >= confidence_threshold:
                result['purpose'] = purpose
                result['purpose_confidence'] = confidence

        # 预测细类
        subcat_predictions = self.predict_subcat(note, category, top_k=1)
        if subcat_predictions:
            subcat, confidence = subcat_predictions[0]
            if confidence >= confidence_threshold:
                result['subcat'] = subcat
                result['subcat_confidence'] = confidence

        return result


def train_classifier_from_feishu(feishu_client, app_token: str, table_id: str,
                                  max_records: int = 5000) -> ExpenseClassifier:
    """
    从飞书表格训练分类器

    :param feishu_client: 飞书客户端
    :param app_token: 应用token
    :param table_id: 表格ID
    :param max_records: 最多使用多少条记录训练
    :return: 训练好的分类器
    """
    logger.info("从飞书表格拉取训练数据...")

    # 拉取最近的记录
    all_records = []
    page_token = None

    while len(all_records) < max_records:
        items, page_token, has_more = feishu_client.list_records(
            app_token=app_token,
            table_id=table_id,
            page_token=page_token,
            page_size=min(500, max_records - len(all_records))
        )

        all_records.extend(items)

        if not has_more:
            break

    logger.info(f"拉取到 {len(all_records)} 条记录")

    # 提取训练数据
    training_data = []
    for record in all_records:
        fields = record.get('fields', {})
        training_data.append({
            '备注': fields.get('备注', ''),
            '分类': fields.get('分类', ''),
            '支出目的': fields.get('支出目的', ''),
            '细类': fields.get('细类', ''),
            '收支': fields.get('收支', '')
        })

    # 训练分类器
    classifier = ExpenseClassifier()
    classifier.train(training_data)

    return classifier


if __name__ == '__main__':
    # 测试代码
    from lib.feishu_client import FeishuClient
    from core.config import Config

    config = Config('config.json')
    mcp_config = config.data.get('mcp_server', {})

    feishu = FeishuClient(
        app_id=mcp_config.get('app_id'),
        app_secret=mcp_config.get('app_secret')
    )

    # 训练分类器
    classifier = train_classifier_from_feishu(
        feishu,
        app_token="P0dsbK6vSaK9vhsOTB2cSwrpnIe",
        table_id="tblGlYT4onKVj3W7",
        max_records=2000
    )

    # 测试预测
    test_cases = [
        ("午餐", "餐饮"),
        ("打车去公司", "交通"),
        ("买书", "学习办公"),
        ("电影票", "娱乐"),
        ("超市购物", "家用"),
    ]

    print("\n=== 预测测试 ===")
    for note, category in test_cases:
        result = classifier.predict(note, category)
        print(f"\n备注: {note}, 分类: {category}")
        print(f"  预测支出目的: {result['purpose']} (置信度: {result['purpose_confidence']:.2%})")
        print(f"  预测细类: {result['subcat']} (置信度: {result['subcat_confidence']:.2%})")
