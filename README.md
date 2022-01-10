# Adverity python challenge
Tested using python3.9

## Install dependencies
```
$ pip install -r requirements.txt
```

## Run migrations and dev server
```
$ cd adverity_challenge
$ python manage.py migrate
$ python manage.py runserver
```

## Run unit tests
```
$ cd adverity_challenge
$ python manage.py tests 
```

## Ways to improve the code
- Switch to asyncio - Probably better solution for fetching multiple requests asynchronously is to by switching to asyncio library - personally never had a chance to use it so I used python standard `concurent` library
- Improve planets fetching - Maybe add some caching for fetching all the planets assuming data for this endpoint doesn't change very often
- Error handling - In current implementation there is **zero** error handling so if anything goes wrong with any of the request app will return ugly 500 error
- Fetch should be POST - Because `/fetch` request does change the data (adding new collection)
- Validation for Value Cunt form - It would be better to switch to django forms to handle any forms in application
