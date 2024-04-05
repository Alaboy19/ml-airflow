from airflow.providers.telegram.hooks.telegram import TelegramHook # импортируем хук телеграма

def send_telegram_success_message(context): # на вход принимаем словарь со контекстными переменными
    hook = TelegramHook(telegram_conn_id='test',
                        token='{7180795511:AAGKeFmehOqXD0p5l3PEjqtImnUfGpZGjQw}',
                        chat_id='{-4156560265}')
    dag = context['dag']
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!' # определение текста сообщения
    hook.send_message({
        
        'chat_id': '{-4156560265}',
        'text': message
            }) 

def send_telegram_failure_message(context):
    hook = TelegramHook(telegram_conn_id='test',
                        token='{7180795511:AAGKeFmehOqXD0p5l3PEjqtImnUfGpZGjQw}',
                        chat_id='{-4156560265}')
    dag = context['dag']
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag} с id={run_id} прошло neуспешно!' # определение текста сообщения
    hook.send_message({
        'chat_id': '{-4156560265}',
        'text': message
         })
