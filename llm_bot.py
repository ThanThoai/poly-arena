from bots.llm_strategy import GeminiBot
import os
import dotenv

dotenv.load_dotenv()

if __name__ == "__main__":
    bot = GeminiBot(
        llm_key=os.getenv("GEMINI_API_KEY"),
        model="gemini-3-flash-preview",
        name="gemini_baseline",
        timeframe="M5",
        api_key=os.getenv("POLYARENA_API_KEY"),
    )
    bot.run()
