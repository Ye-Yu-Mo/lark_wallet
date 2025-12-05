"""
健康建议 Prompt 构建器
构建发送给 Deepseek 的提示词
"""
from typing import Dict, List
from datetime import datetime


def build_health_advice_prompt(
    profile: Dict,
    health_records: List[Dict],
    meals: List[Dict],
    exercises: List[Dict],
    fridge_inventory: List[Dict],
    expiring_ingredients: List[Dict],
    is_workday: bool
) -> str:
    """
    构建健康建议 Prompt

    :param profile: 个人健康档案
    :param health_records: 最近的健康记录
    :param meals: 最近的饮食记录
    :param exercises: 最近的运动记录
    :param fridge_inventory: 冰箱库存
    :param expiring_ingredients: 即将过期的食材
    :param is_workday: 是否工作日
    :return: 完整的 prompt
    """

    # 1. 个人信息部分
    profile_text = _format_profile(profile)

    # 2. 健康记录部分
    health_text = _format_health_records(health_records)

    # 3. 饮食记录部分
    meals_text = _format_meals(meals)

    # 4. 运动记录部分
    exercises_text = _format_exercises(exercises)

    # 5. 冰箱库存部分
    fridge_text = _format_fridge_inventory(fridge_inventory, expiring_ingredients)

    # 6. 当前情况部分
    context_text = _format_context(is_workday)

    # 7. 组合完整 prompt
    prompt = f"""# 角色定义

你是一名顶级临床营养师,有超过 20 年严肃医学背景,研究过几千份血检、饮食记录与慢性病案例。你的工作不是"劝人多吃蔬菜",而是拆穿饮食中的蠢逻辑、伪科学、烂思路,把营养问题逼回生理学和数据层面。

你讨厌玄学饮食法,讨厌"听说这样更健康",讨厌不经思考的营养恐慌。

你的职责很简单:找蠢点、拆逻辑、讲生理、保留真正有效的营养策略。

## 核心哲学

1. **好品味 = 不制造伪需求**: 90% 的'营养问题'不是问题,是人自己瞎折腾出来的
2. **Never break physiology**: 吃法不能违背荷尔蒙、代谢与神经系统的底层规律
3. **实用主义**: 不能长期坚持 = 没用,不匹配生活方式 = 没用
4. **简洁执念**: 规则越复杂,越注定崩溃

## 沟通风格

- 短、硬、不废话
- 观点蠢就说蠢,并说明为什么蠢
- 只讲事实,不讲情绪

---

# 用户信息

{profile_text}

{health_text}

{meals_text}

{exercises_text}

{fridge_text}

{context_text}

---

# 任务

请根据以上信息,用你的专业视角提供建议。记住:

1. **不要制造营养焦虑**,如果数据看起来正常,就说正常
2. **拆穿蠢逻辑**,如果发现饮食模式有问题,直接指出
3. **讲生理机制**,而不是"多吃XX对身体好"这种空话
4. **保持简洁**,别搞 10 条规则,3 条足够

请提供以下建议(用HTML格式):

## 1. 今晚做什么吃?
- 基于当前库存给出 2-3 个菜品建议
- 优先使用即将过期的食材
- **每个菜品必须包含：**
  - **食材清单（具体克数）**：例如 "鸡胸肉150g、西兰花200g、大蒜10g"
  - **简单制作方法（3-5步）**：不要废话，直接说怎么做
- 如果最近饮食结构有问题,在这里纠正

## 2. 采购建议
- 明天需要买什么食材?
- 如果库存已经够用,就说够用,别制造伪需求

## 3. 运动建议
- 基于最近运动情况和健康目标
- 如果运动量已经足够,就说足够
- 如果运动频率有问题,指出生理层面的原因

## 4. 健康评估
- 最近体重变化趋势分析(如果有明显问题,说明生理原因)
- 饮食结构评估(找出真正的问题,而不是伪问题)
- 需要改进的地方(最多 2 条,多了执行不了)

**重要**:
- 用简单的 HTML 标签 (如 <p>, <ul>, <li>, <b>, <h3>)
- 用简洁、直接的语言,避免空话套话和营养鸡汤
- 不要使用 Markdown 格式
"""

    return prompt


def _format_profile(profile: Dict) -> str:
    """格式化个人档案"""
    if not profile:
        return "## 个人信息\n暂无数据"

    name = profile.get('姓名', '用户')
    birth_date = profile.get('出生日期')
    height = profile.get('身高(cm)', 0)
    target_weight = profile.get('目标体重(kg)', 0)
    target_body_fat = profile.get('目标体脂率(%)', 0)
    food_preference = profile.get('饮食偏好', '')

    # 计算年龄
    age = "未知"
    if birth_date:
        try:
            birth_dt = datetime.fromtimestamp(birth_date / 1000)
            age = datetime.now().year - birth_dt.year
        except:
            pass

    text = f"""## 个人信息
- 姓名: {name}
- 年龄: {age}岁
- 身高: {height}cm
- 目标体重: {target_weight}kg
- 目标体脂率: {target_body_fat}%"""

    if food_preference:
        text += f"\n- 饮食偏好: {food_preference}"

    return text


def _format_health_records(records: List[Dict]) -> str:
    """格式化健康记录"""
    if not records:
        return "## 最近健康记录\n暂无数据"

    text = "## 最近健康记录\n"

    for record in records:
        date = record.get('日期')
        weight = record.get('体重(kg)', 0)
        body_fat = record.get('体脂率(%)', 0)
        note = record.get('备注', '')

        date_str = _format_timestamp(date)

        text += f"- {date_str}: 体重 {weight}kg"
        if body_fat:
            text += f", 体脂率 {body_fat}%"
        if note:
            text += f" ({note})"
        text += "\n"

    return text


def _format_meals(meals: List[Dict]) -> str:
    """格式化饮食记录"""
    if not meals:
        return "## 最近饮食记录\n暂无数据"

    text = "## 最近饮食记录\n"

    # 按日期分组
    meals_by_date = {}
    for meal in meals:
        date = meal.get('日期')
        date_str = _format_timestamp(date, format='date')

        if date_str not in meals_by_date:
            meals_by_date[date_str] = []

        meal_type = meal.get('餐次', '未知')
        food = meal.get('食物描述', '')
        location = meal.get('地点', '')

        meals_by_date[date_str].append({
            'type': meal_type,
            'food': food,
            'location': location
        })

    # 输出
    for date_str in sorted(meals_by_date.keys(), reverse=True):
        text += f"\n**{date_str}**\n"
        for meal in meals_by_date[date_str]:
            text += f"- {meal['type']}"
            if meal['location']:
                text += f" ({meal['location']})"
            text += f": {meal['food']}\n"

    return text


def _format_exercises(exercises: List[Dict]) -> str:
    """格式化运动记录"""
    if not exercises:
        return "## 最近运动记录\n暂无数据"

    text = "## 最近运动记录\n"

    for exercise in exercises:
        date = exercise.get('日期')
        exercise_type = exercise.get('运动类型', '未知')
        duration = exercise.get('时长(分钟)', 0)
        distance = exercise.get('距离(km)', 0)
        note = exercise.get('备注', '')

        date_str = _format_timestamp(date)

        text += f"- {date_str}: {exercise_type} {duration}分钟"
        if distance:
            text += f", {distance}km"
        if note:
            text += f" ({note})"
        text += "\n"

    return text


def _format_fridge_inventory(inventory: List[Dict], expiring: List[Dict]) -> str:
    """格式化冰箱库存"""
    if not inventory:
        return "## 冰箱库存\n暂无数据"

    text = "## 冰箱库存\n"

    # 按分类分组
    by_category = {}
    for item in inventory:
        name = item.get('食材名称', '未知')
        quantity = item.get('数量描述', '')
        category = item.get('分类', '其他')
        expire_date = item.get('过期日期')

        if category not in by_category:
            by_category[category] = []

        item_text = f"{name}"
        if quantity:
            item_text += f" ({quantity})"

        # 标记即将过期
        if expire_date and any(e.get('食材名称') == name for e in expiring):
            expire_str = _format_timestamp(expire_date, format='date')
            item_text += f" **[即将过期: {expire_str}]**"

        by_category[category].append(item_text)

    # 输出
    for category, items in by_category.items():
        text += f"\n**{category}**\n"
        for item in items:
            text += f"- {item}\n"

    return text


def _format_context(is_workday: bool) -> str:
    """格式化当前情况"""
    today = datetime.now().strftime('%Y年%m月%d日')
    weekday = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][datetime.now().weekday()]

    text = f"""## 当前情况
- 日期: {today} ({weekday})
- 类型: {'工作日' if is_workday else '休息日'}
- 晚餐: 在家做饭"""

    return text


def _format_timestamp(timestamp: int, format: str = 'datetime') -> str:
    """
    格式化时间戳

    :param timestamp: 毫秒时间戳
    :param format: 格式类型 (datetime/date)
    :return: 格式化后的字符串
    """
    try:
        dt = datetime.fromtimestamp(timestamp / 1000)

        if format == 'date':
            return dt.strftime('%m月%d日')
        else:
            return dt.strftime('%m月%d日 %H:%M')
    except:
        return "未知"
