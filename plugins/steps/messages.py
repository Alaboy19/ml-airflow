import os 
from airflow.providers.telegram.hooks.telegram import TelegramHook # импортируем хук телеграма

TOKEN_TELEGRAM = os.getenv('TELEGRAM_BOT_TOKEN')
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

def send_telegram_success_message(context): # на вход принимаем словарь со контекстными переменными
    hook = TelegramHook(telegram_conn_id='test',
                        token='{TOKEN_TELEGRAM}',
                        chat_id='{CHAT_ID}')
    dag = context['dag']
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!' # определение текста сообщения
    hook.send_message({
        
        'chat_id': '{CHAT_ID}',
        'text': message
            }) 

def send_telegram_failure_message(context):
    hook = TelegramHook(telegram_conn_id='test',
                        token='{TOKEN_TELEGRAM}',
                        chat_id='{CHAT_ID}')
    dag = context['dag']
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag} с id={run_id} прошло neуспешно!' # определение текста сообщения
    hook.send_message({
        'chat_id': '{CHAT_ID}',
        'text': message
         })
