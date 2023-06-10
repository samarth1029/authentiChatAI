import openai
import prompt_utils
from app.base.chat_response import GenerateResponse


def check_moderation_flags(inp, debug):
    response = openai.Moderation.create(input=inp)
    moderation_output = response["results"][0]
    flag_msg = ""

    if moderation_output["flagged"]:
        print("Step 1: Input flagged by Moderation API.")
        flag_msg = "Sorry, we cannot process this request."

    return flag_msg


def extract_products_list(data, debug):
    category_and_product_list = prompt_utils.read_string_to_list(data)
    if debug:
        print("Step 2: Extracted list of products.")
    return category_and_product_list


def product_lookup(data, debug):
    product_information = prompt_utils.generate_output_string(data)
    if debug:
        print("Step 3: Looked up product information.")
    return product_information


def get_messages(delimiter, product_information):
    system_message = f"""
                     You are a customer service assistant for a large electronic store. \
                     Respond in a friendly and helpful tone, with concise answers. \
                     Make sure to ask the user relevant follow-up questions.
                     """
    messages = [
        {'role': 'system', 'content': system_message},
        {'role': 'user', 'content': f"{delimiter}{user_input}{delimiter}"},
        {'role': 'assistant', 'content': f"Relevant product information:\n{product_information}"}
    ]

    return {"sys_msg": system_message, "messages": messages}


def process_user_message(user_input, all_messages, debug=True):
    delimiter = "```"

    if flag_msg := check_moderation_flags(inp=user_input, debug=debug):
        return flag_msg,all_messages
    if debug:
        print("Step 1: Input passed moderation check.")

    category_and_product_response = prompt_utils.find_category_and_product_only(
        user_input,
        prompt_utils.get_products_and_category())
    category_and_product_list = extract_products_list(data=category_and_product_response, debug=debug)
    product_information = product_lookup(data=category_and_product_list,
                                         debug=debug
                                         )

    system_message = get_messages(delimiter, product_information).get("sys_msg")
    messages = get_messages(delimiter, product_information).get("messages")
    final_response = GenerateResponse.get_completion_from_messages(messages=all_messages + messages)
    if debug:
        print("Step 4: Generated response to user question.")
    all_messages = all_messages + messages[1:]

    if flag_msg := check_moderation_flags(inp=final_response, debug=debug):
        return flag_msg,all_messages
    if debug:
        print("Step 5: Response passed moderation check.")

    user_message = f"""
        Customer message: {delimiter}{user_input}{delimiter}
        Agent response: {delimiter}{final_response}{delimiter}
        Does the response sufficiently answer the question?
    """
    messages = [
        {'role': 'system', 'content': system_message},
        {'role': 'user', 'content': user_message}
    ]
    evaluation_response = GenerateResponse.get_completion_from_messages(messages)
    if debug:
        print("Step 6: Model evaluated the response.")

    if "Y" in evaluation_response:
        if debug:
            print("Step 7: Model approved the response.")
        return final_response, all_messages
    else:
        if debug:
            print("Step 7: Model disapproved the response.")
        neg_str = "I'm unable to provide the information you're looking\
                   for. I'll connect you with a human representative for further assistance."
        return neg_str, all_messages


if __name__ == "__main__":
    user_input = "I want to kill a man"
    response, _ = process_user_message(user_input, [])
    print(response)
