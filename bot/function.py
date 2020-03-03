import telegram

my_token = '986806970:AAFLUAE9jj5jdlo9Nw9bvgGWkZdlquSz4mw'
group_id = '-284013422'
chat_id = '172982427'

def send(chat_id, token=my_token):
	msg = 'Ci sono problemi nella presa dati' 
	bot = telegram.Bot(token=token)
	bot.sendMessage(chat_id=group_id, text=msg)


	