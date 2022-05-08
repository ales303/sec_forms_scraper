from fake_useragent import UserAgent

try:
	ua = UserAgent()
	userAgent = ua.random
	print("User Agent: ", userAgent)
except Exception as e:
	print(e)

try:
	ua = UserAgent(verify_ssl=False)
	userAgent = ua.random
	print("User Agent: ", userAgent)
except Exception as e:
	print(e)

