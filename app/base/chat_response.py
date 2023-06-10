import os
import openai

from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())
openai.api_key = os.environ['OPENAI_API_KEY']


class GenerateResponse:
    @staticmethod
    def get_completion_from_messages(messages, model="gpt-3.5-turbo",
                                     temperature=0,
                                     max_tokens=500):
        response = openai.ChatCompletion.create(
            model=model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        return response.choices[0].message["content"]

    @staticmethod
    def collect_messages(prompt, debug=False):
        from app.base.process_user_message import process_user_message
        if debug:
            print(f"{prompt=}")
        if prompt == "":
            return
        prompt = ''
        global context
        response, context = process_user_message(prompt, context, debug=False)
        context.append({'role': 'assistant', 'content': f"{response}"})
