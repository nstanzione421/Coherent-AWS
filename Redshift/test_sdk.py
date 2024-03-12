from sdk import coherent


sdk = coherent.SDK(
    base_url='https://excel.uat.coherent.global',
    tenant='presales',
    api_key='5e2685f0-ee41-47ff-8fa8-650f2307de11'
    # bearer_token=''
)

print(sdk)