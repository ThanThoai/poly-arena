This specification outlines the **Achievement & Badge System** for the PolyArena platform. It is designed to track behavioral patterns, trading performance, and humorous "edge cases" for both human users and AI bots.

---

# Specification: PolyArena Achievement & Badge System

## 1. System Architecture

The achievement system operates on an **Asynchronous Observer Pattern**. Every time a trade is settled (Binary Option resolution), an `AchievementService` evaluates the trade metadata against a set of predefined logical rules.

### 1.1 Inheritance Logic (Bot-to-User)

* **Ownership:** Every Bot is owned by a User (`owner_id`).
* **Achievement Propagation:** When a **Bot** triggers the logic for an achievement, the achievement is recorded for the **Bot**, and the **User** is credited as the "Owner of the [Badge Name]".
* **Display:** On the Leaderboard, if a User's bot is high-ranking, the User's profile displays the bot's trophies.

---

## 2. Database Schema (Conceptual)

### `achievement_definitions`

* `id`: UUID
* `slug`: String (e.g., `night-owl`)
* `name`: String
* `description`: String (Witty/Humorous)
* `tier`: Enum (`BRONZE`, `SILVER`, `GOLD`, `PLATINUM`)
* `logic_gate`: JSON (Parameters for the coding agent)

### `bot_achievements`

* `id`: UUID
* `bot_id`: UUID (FK)
* `achievement_id`: UUID (FK)
* `earned_at`: Timestamp
* `metadata`: JSON (The specific trade data that triggered the win)

---

## 3. Achievement Categories & Logic

### 3.1 The "ICT Time-Lord" Category (Time-Based)

*Logic is calculated based on **UTC+7 (ICT)**.*

| Badge Slug | Name | Humorous Description | Logic Requirements |
| --- | --- | --- | --- |
| `insomniac-owl` | **The Insomniac Owl** | "Are these candles or hallucinations?" | Place > 10 trades between **02:00 - 04:00 ICT**. |
| `stealth-employee` | **Stealth Mode: ON** | "Boss is behind you. Trade faster." | Place > 5 trades per hour between **09:00 - 11:00 ICT**, Monday-Friday. |
| `lunch-break-gambler` | **Lunch Break Legend** | "Who needs food when you have volatility?" | Place an 'All-in' bet (90% balance) between **12:00 - 13:00 ICT**. |
| `weekend-warrior` | **Weekend Warrior** | "Touch grass? Never heard of that ticker." | 100% of weekly volume occurs on Saturday & Sunday. |

### 3.2 The "Size Matters" Category (Volume & Stake)

| Badge Slug | Name | Humorous Description | Logic Requirements |
| --- | --- | --- | --- |
| `penny-pincher` | **Chúa Tể Cò Con** | "50 trades for the price of one Banh Mi." | 50 consecutive trades at the minimum allowable stake. |
| `china-shop-whale` | **Whale in a China Shop** | "You didn't just trade; you broke the chart." | A single Market Order that moves the Price > 5%. |
| `pink-slip-seeker` | **Sổ Đỏ Diver** | "One green candle to rule them all, or back to the streets." | A single trade stake > 90% of total wallet balance. |
| `liquidity-thief` | **Liquidity Ninja** | "You came, you saw, you emptied the book." | A single Market Order that sweeps > 3 price levels (Slippage). |

### 3.3 The "Binary Trauma" Category (BO Specific Results)

| Badge Slug | Name | Humorous Description | Logic Requirements |
| --- | --- | --- | --- |
| `clutch-god` | **The 89th Minute Clutch** | "Your heart is made of polymer." | Win a trade due to a price reversal in the final 1% of the session time. |
| `heart-attack` | **Sudden Cardiac Arrest** | "Winning for 59 minutes, losing at 59:59." | Lose a trade where the prediction was correct until the final 1 second. |
| `golden-incense` | **Bát Nhang Vàng** | "A beacon of hope for people betting against you." | Maintain a loss streak of 10 consecutive settled trades. |
| `immortal-sniper` | **The Immortal Sniper** | "Winning is easy when you're this lucky." | Win 5 consecutive trades with an entry price < $0.15 (High Risk). |
| `perfect-random` | **Chúa Tể Tung Đồng Xu** | "Statistically, you are a literal coin flip." | Maintain exactly 50% Win Rate after 100+ trades. |

---

## 4. Execution Logic for Coding Agents

### 4.1 Triggering Evaluation

```python
def on_trade_resolved(trade_data):
    # 1. Update Bot Statistics
    update_bot_stats(trade_data.bot_id)
    
    # 2. Check for newly unlocked achievements
    new_badges = check_achievement_logic(trade_data.bot_id)
    
    # 3. If badge unlocked, notify User
    for badge in new_badges:
        award_badge(trade_data.bot_id, badge)
        broadcast_to_ui(trade_data.owner_id, f"Your Bot [{bot_name}] earned the [{badge.name}] badge!")

```

### 4.2 Bot-to-User UI Display Logic

When fetching the User's Profile:

```sql
SELECT DISTINCT ad.name, ad.slug, b.name as earned_by_bot
FROM achievement_definitions ad
JOIN bot_achievements ba ON ad.id = ba.achievement_id
JOIN bots b ON ba.bot_id = b.id
WHERE b.owner_id = :current_user_id
ORDER BY ba.earned_at DESC;

```

---

## 5. Summary Matrix for Developers

* **Timezone Reference:** Always convert UTC timestamps to `Asia/Ho_Chi_Minh` before evaluating "Time-Lord" badges.
* **Precision:** Use `Decimal` for ROI and Stake calculations to avoid floating-point errors in "Sổ Đỏ Diver" detection.
* **Notification:** Use a "Toast" or "Pop-up" on the Frontend with the witty description to maximize user engagement.

This update expands the **PolyArena Achievement System** with four specialized categories: **"Banter & Bad Luck"**, **"Vagabond Style"**, **"Holy Prophet & Assassin"**, and **"How the Steel Was Tempered"**.

These additions are designed to capture the emotional highs and lows of Binary Options (BO) trading where outcomes are absolute ($1.0 or $0.0).

---

# Specification Addendum: Achievement Categories (Expansion Pack)

## 6. Achievement Group: "Banter & Bad Luck" (Cà Khịa & Đen Đủi)

*Focus: Humiliating losses, "clown" moves, and statistical anomalies of bad luck.*

| Badge Slug | Name | Humorous Description | Logic Requirements |
| --- | --- | --- | --- |
| `peak-buyer` | **Đại Sứ Đu Đỉnh** | "The view is great from up here, isn't it?" | Purchase a token at **Price > $0.98** and the result is **LOSS**. |
| `jinxed-finger` | **Tín Hiệu Ngược Uy Tín** | "The market moves just to spite you." | Price moves **opposite** to the trade direction within 5 seconds of entry 10 times in a row. |
| `clown-flip` | **Kẻ Phản Lưới Nhà** | "Switching sides didn't help. You're just wrong twice." | User/Bot sells YES to buy NO (or vice-versa) within the same session, and **still loses**. |
| `anti-midas` | **Bàn Tay Midas Ngược** | "Everything you touch turns to... well, not gold." | Reach a **Loss Streak of 15** settled trades across different markets. |

---

## 7. Achievement Group: "Vagabond Style" (Phong Cách Bụi Đời)

*Focus: Chaotic behavior, low-budget "dust" trading, and erratic patterns.*

| Badge Slug | Name | Humorous Description | Logic Requirements |
| --- | --- | --- | --- |
| `dust-collector` | **Vua Ve Chai** | "Collecting pennies like they're infinity stones." | Execute 100 trades where the total stake is **< $1.00 each**. |
| `fidget-spinner` | **Kẻ Lật Mặt Như Bánh Tráng** | "Do you even have a plan, or just itchy fingers?" | Change prediction (Buy/Sell/Cancel) **> 5 times** in a single 15-minute BO session. |
| `ghost-in-shell` | **Thánh Ngủ Quên** | "Woke up a millionaire, or a beggar. Who knows?" | Place a trade and perform **zero platform activity** for > 7 days until resolution. |
| `chaos-theory` | **Chúa Tể Random** | "Even the AI can't predict your next move." | Trades show **no correlation** to market trends (Random entries) but result in a 50% win rate. |

---

## 8. Achievement Group: "Holy Prophet & Assassin" (Thánh Dự & Sát Thủ)

*Focus: High-precision entries, high-risk wins, and "sniping" behavior.*

| Badge Slug | Name | Humorous Description | Logic Requirements |
| --- | --- | --- | --- |
| `blind-sniper` | **Sát Thủ Mù** | "You didn't see the chart, but you felt the win." | Win a trade with **Price < $0.10** at entry (High Risk/High Reward). |
| `oracle-vision` | **Nhà Tiên Tri Vũ Trụ** | "Are you from the future? Blink once for YES." | Maintain a **90% Win Rate** over a minimum of 50 settled trades. |
| `surgical-strike` | **Nhát Dao Chí Mạng** | "One shot, one kill. Efficiency 100%." | Place exactly **one trade** per market and win 10 markets consecutively. |
| `shadow-killer` | **Sát Thủ Thầm Lặng** | "Moved the profit, didn't move the price." | Win a trade with a **Volume > $1,000** that resulted in **< 0.1% Slippage**. |

---

## 9. Achievement Group: "How the Steel Was Tempered" (Thép Đã Tôi Thế Đấy)

*Focus: Resilience, surviving drawdowns, and extreme "all-or-nothing" outcomes.*

| Badge Slug | Name | Humorous Description | Logic Requirements |
| --- | --- | --- | --- |
| `diamond-soul` | **Lì Hơn Cả Sàn** | "Watched it go to $0.01, waited for the $1.00." | Position hit an **unrealized loss of -95%** but held until it became a **WIN**. |
| `phoenix-down` | **Trỗi Dậy Từ Tro Tàn** | "Account balance: $0.01. Current status: Legend." | Recover from a total wallet balance of **< $1.00** back to the **initial starting balance**. |
| `the-martyr` | **Kẻ Tử Vì Đạo** | "You went down with the ship. Respect." | Lose a trade where the stake was **100% of the wallet** (Total Bankruptcy). |
| `iron-will` | **Ý Chí Thép** | "Not even a 20-loss streak could stop this bot." | Continue trading immediately after a **Loss Streak of 10+** without changing stake size. |

---

## 10. Implementation Notes for Coding Agent

### 10.1 Logic Trigger: `Diamond Soul` (The Resilience Check)

The agent must monitor the **historical low** of a position's value during its lifecycle.

```python
def evaluate_diamond_soul(position_history):
    entry_price = position_history[0].price
    min_market_price = min([p.market_price for p in position_history])
    final_status = position_history[-1].status
    
    if min_market_price <= (entry_price * 0.05) and final_status == "WIN":
        return True
    return False

```

### 10.2 Logic Trigger: `ICT Time-Lord` vs `ICT Location`

Ensure the server handles the offset correctly for **ICT (UTC+7)** regardless of host location.

```python
from datetime import datetime
import pytz

def get_ict_hour():
    utc_now = datetime.now(pytz.utc)
    ict_now = utc_now.astimezone(pytz.timezone('Asia/Ho_Chi_Minh'))
    return ict_now.hour

```

### 10.3 Propagation Reminder

When a `bot_id` earns any of the above, the system must trigger a notification to the `owner_id` (the User) and update the User's aggregate trophy cabinet.
