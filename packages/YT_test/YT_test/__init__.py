# import os
# import json
# import pkgutil
# import logging
#
# path = "{}/google-cloud-storage-credentials.json".format(os.getcwd())
#
# credentials_content = '''
#     {\r\n  \"type\": \"service_account\",\r\n  \"project_id\": \"ekstepspeechrecognition\",\r\n  \"private_key_id\": \"76e44673ef49ac12b7cb2aa094920e6df876bcab\",\r\n  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQC7joxDM8uVFf+L\\nMbdMANKvhff2QQEiwfWxhfnXrNq\/s44d\/fR\/dbAX35ajTrpOXJFIEuY29gTADzrd\\nMpixn+lMOWAaHeWM2AkUKswHcoVvsAqvAhSZUahg5PaGcWiIWJP3PyNPca0Qx6+o\\nrnse2S7Omhv1vEloAxhUWtxIgi6WJfR8KwmLhqVH6mUGkqVJf0l9wzQUVc+jDaFI\\nzOwKl+qOlHzt9ynLqmqrBF3C5RozAfVTqdqusahD463hVKQy+LOKE3I5iWRUQUcE\\nonrqMFrOILF0NHNvqBglPJ9iRqjc\/m93j\/5KniyikNzedXHJ1NSbn07oDdzis9hh\\n23tsiD7fAgMBAAECggEARQGbAKS\/bBBmb5+wmXGaEsNfKobbNJ8ZVyH8fQpXh324\\nNbe4q+awjfARO++c43TybQqrEiCtOb7AwR67CGtWCln3zlQen5XirT1byQetKZ0j\\nKSXCT3C4W0ISo\/943uV8N1VPGA0yiEB4FD9yBDUTICeaTuziMzckTfEKKFFhc5NN\\nJKK3jyjTuz8UiLtK3KTRzExNs3QfibwhIdV\/eDOAGc+VT61ec4Fdh+ejYC3vK+Mn\\nO68Jnnah\/TuyaBLVMW3YsyXQg+oVsPxBP6T7UzF6sbbBkcg6\/U\/URv2\/5Vc1k\/T4\\n6cFInJIgLtHQvxRLILtMNAx9agQ1PVubl6fHD44zTQKBgQDcK6QFJzKqo8kZ5IqW\\nGaLAwZlLeT9VmJ6Z950bPW2a\/sqH0XFTjNPVV7FFKEm\/DfJY4S5IyzKo9zGsMPEL\\n8Zc9zOz4dOkIwNzGZ2ycCkvrTn8b7wRRQAsiAWM0\/A4MRj4v5bRLmZSg4wKmTMti\\nRxdXqVG02ZQJmoZjRbk\/xM3lnQKBgQDaFDbbhKWn8gBvv0\/kbDorpRDIorZs+8Ud\\n\/A8g1xI3xBHiuxWT5tugPfo+395D7BT6CBZ+aBk2qwu2baH7bNG\/iXjuGa7Ay+mH\\nmIVHsCUX+eFITvbeT8t9Ncv\/P03ER0RbFL8eZW\/n8i1KcIy9ZN7N+e9EBb3\/wAiL\\nA9wMNyCrqwKBgQCPdOgEa4v534pTErSyJLYFPp\/xq2j3DuCYldyKOTZHfajdYjyj\\nIemM4vyggSW8FQxJmT+dMrkpmxeEiMcm7x2KqRHmudZ1W6T+qbj820Coa5cqzkxT\\n3JTkbV8E0Q8eNE6kytj1QXa0dfXuAa+rs4KkHbEdU3+\/2i2iVXXk9QjriQKBgQCS\\nVigti8hBd0HVurHYnMs4CE7H42+4mAXAxig8qDVgWGCMHXAwTCSqVYx77mtOdrfo\\nw86cSixJI+P7KXwdo\/rnpU8Rrwg19V8ijzU4UrnBaftDM0GzEiaBQb0+7XK4t\/3l\\nhHlu4zCBm1\/K6NV4LZzY6NMmeRfy6yCQcCmTxNZWewKBgQDCa1XMJs6i534ye2TC\\nQ\/skkl6X3HMJrJ6B3pg4zon6FV\/C6E\/1+PumAK69aXDAtSN4lbd2\/wUmGZOazqWc\\nKMXa2+KTBId4YvrYoKyYGufpNecaGU+hn2hDNNWPM07U7zX94tX6ATaiMJFUs6ns\\nLgLG84ts++dTaMFip9Rh8uc44g==\\n-----END PRIVATE KEY-----\\n\",\r\n  \"client_email\": \"servacct-speechrecognition-crw@ekstepspeechrecognition.iam.gserviceaccount.com\",\r\n  \"client_id\": \"115727090172355048355\",\r\n  \"auth_uri\": \"https:\/\/accounts.google.com\/o\/oauth2\/auth\",\r\n  \"token_uri\": \"https:\/\/oauth2.googleapis.com\/token\",\r\n  \"auth_provider_x509_cert_url\": \"https:\/\/www.googleapis.com\/oauth2\/v1\/certs\",\r\n  \"client_x509_cert_url\": \"https:\/\/www.googleapis.com\/robot\/v1\/metadata\/x509\/servacct-speechrecognition-crw%40ekstepspeechrecognition.iam.gserviceaccount.com\"\r\n}\r\n
# '''
#
# with open(path, "w") as text_file:
#     text_file.write(credentials_content)
#
# logging.warning("Path to credentials: %s" % path)
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path
