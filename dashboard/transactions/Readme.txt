There are two scripts in this directory:
- creditCardNumberGenerator
- DummyCreditCardTransactionGenerator

Configurations for these can be done in dataGenerator.config

creditCardNumberGenerator:
This will generate new credit cards with customer's details. 
Currently this has been run & 10000 card details have been generated to be used.
This is a one-time script to just generate new card details. 
NOT TO BE CONFUSED WITH TRANSACTION GENERATION
To run this script: python creditCardNumberGenerator.py

DummyCreditCardTransactionGenerator:
This will generate new transactions with the card details from cards.csv (output of creditCardNumberGenerator)
Control the rate of transaction generation using 'time_delay_in_seconds' field in the configuration.
0 > time_delay_in_seconds 
time_delay_in_seconds can be a float value as well
To run this script: python DummyCreditCardTransactionGenerator.py

Transaction file schema:
card_no,age,gender,card_type,city,state,txn_datetime,amount,expense_type