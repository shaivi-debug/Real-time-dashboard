#!/usr/bin/env python
# coding: utf-8

# In[1]:


from random import Random
import copy
import pandas as pd
import random
import os
import configparser


# In[2]:


config = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
config.read(os.path.abspath('dataGenerator.config'))


# In[3]:


visaPrefixList = [
        ['4', '5', '3', '9'],
        ['4', '5', '5', '6'],
        ['4', '9', '1', '6'],
        ['4', '5', '3', '2'],
        ['4', '9', '2', '9'],
        ['4', '0', '2', '4', '0', '0', '7', '1'],
        ['4', '4', '8', '6'],
        ['4', '7', '1', '6'],
        ['4']]

mastercardPrefixList = [
        ['5', '1'], ['5', '2'], ['5', '3'], ['5', '4'], ['5', '5']]

amexPrefixList = [['3', '4'], ['3', '7']]

discoverPrefixList = [['6', '0', '1', '1']]

dinersPrefixList = [
        ['3', '0', '0'],
        ['3', '0', '1'],
        ['3', '0', '2'],
        ['3', '0', '3'],
        ['3', '6'],
        ['3', '8']]

enRoutePrefixList = [['2', '0', '1', '4'], ['2', '1', '4', '9']]

jcbPrefixList = [['3', '5']]

voyagerPrefixList = [['8', '6', '9', '9']]


# In[4]:


def completed_number(prefix, length):
    """
    'prefix' is the start of the CC number as a string, any number of digits.
    'length' is the length of the CC number to generate. Typically 13 or 16
    """

    ccnumber = prefix

    # generate digits

    while len(ccnumber) < (length - 1):
        digit = str(generator.choice(range(0, 10)))
        ccnumber.append(digit)

    # Calculate sum

    sum = 0
    pos = 0

    reversedCCnumber = []
    reversedCCnumber.extend(ccnumber)
    reversedCCnumber.reverse()

    while pos < length - 1:

        odd = int(reversedCCnumber[pos]) * 2
        if odd > 9:
            odd -= 9

        sum += odd

        if pos != (length - 2):

            sum += int(reversedCCnumber[pos + 1])

        pos += 2

    # Calculate check digit

    checkdigit = ((sum / 10 + 1) * 10 - sum) % 10

    ccnumber.append(str(checkdigit))

    return ''.join(ccnumber)


# In[5]:


def credit_card_number(rnd, prefixList, length, howMany):

    result = []

    while len(result) < howMany:

        ccnumber = copy.copy(rnd.choice(prefixList))
        result.append(completed_number(ccnumber, length))

    return result


# In[6]:


def output(title, numbers):

    result = []
    result.append(title)
    result.append('-' * len(title))
    result.append('\n'.join(numbers))
    result.append('')

    return '\n'.join(result)


# In[7]:


generator = Random()
generator.seed() 


# In[8]:


gender=['M','F']
card_types=['Silver','Gold','Platinum','Signature']


# In[9]:


cities=pd.read_csv(os.path.abspath('cities.csv'))


# In[10]:


def get_card_details():
    fetched_city=cities.sample()
    card_detail=pd.DataFrame({"card_no":[credit_card_number(generator, mastercardPrefixList, 16, 1)[0].split('.')[0]],
              "age":[random.randint(18, 60)],
              "gender":[random.choice(gender)],
              "card_type":[random.choice(card_types)],
               "city":fetched_city['city'],
               "state":fetched_city['state']})
    return card_detail


# In[11]:


no_of_cards_to_generate=config.get('card_generator', 'number_of_cards_to_generate')


# In[12]:


df_card_details = pd.DataFrame(columns = ['card_no', 'age', 'gender','card_type','city','state'])


# In[13]:


def generate_cards(no_of_cards):
    df_card_details = pd.DataFrame(columns = ['card_no', 'age', 'gender','card_type','city','state'])
    for i in range(int(no_of_cards_to_generate)):
        df_card_details=df_card_details.append(get_card_details())
    return df_card_details


# In[14]:


dft=generate_cards(no_of_cards_to_generate)


# In[15]:


if(len(dft.card_no.unique())==int(no_of_cards_to_generate)):
    dft.to_csv('cards.csv',sep=',',encoding='utf-8',index=False)
else:
    print("Too many cards expected, ran out of unique combinations")


# In[ ]:




