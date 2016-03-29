#!get_street.py

## use re to go parse out the street name,
## return nothing if street name cannot be found
import re

def first_upper(street_name):
    name_parts= [i[0].upper()+i[1:] for i in street_name.split()]
    ret=""
    for i in name_parts:
        ret+=i+" "
    ret+="*!*"
    ret = ret.replace(" *!*","")
    return ret


## function to replace the street type with the correct format...
def clean_street_type(street_type):
    street_type=street_type.strip(" ")
    street_type_dict={
        "av.":" avenue",
        "ave.":"avenue",
        "ave":" avenue",
        "av":" avenue",
        "st":" street",
        "st.": " street",
        "rd":" road",
        "rd.":" road",
        "tr":" terrace",
        "dr":" drive",
        "dr.":" drive",
        "blvd.":"boulivard",
        "blvd":" boulivard",
        "pl": " place",
        "pl.": " place"
    }
    if street_type in street_type_dict:
        street_type=street_type_dict[street_type]
    else:
        street_type = " "+street_type
    return street_type

def get_street_name(address):
    ##regular expression to match parsable address format
    address_expression = re.compile('^\d+(-|/)?\d*\s.*(\
(\sst\.?($|\s|(reet($|\s))))\
|(\save?\.?($|\s|(enue($|\s))))\
|(\sr((d\.?($|\s))|(oad($|\s))))\
|(\sdr\.?($|\s|(ive($|\s))))\
|(\spl\.?($|\s|(ace($|\s))))\
|(\sterrace)\
|(\sb((lvd\.?($|\s))|(oulivard($|\s))))\
|(\spkwy\.?|parkway)\
)')

    ##regular expression to extract street name
    street_type_expression = re.compile("(\
(\sst\.?($|\s|(reet($|\s))))\
|(\save?\.?($|\s|(enue($|\s))))\
|(\sr((d\.?($|\s))|(oad($|\s))))\
|(\sdr\.?($|\s|(ive($|\s))))\
|(\spl\.?($|\s|(ace($|\s))))\
|(\sterrace)\
|(\sb((lvd\.?($|\s))|(oulivard($|\s))))\
|(\spkwy\.?|parkway)\
)")

    ##regular expression to extract street number
    street_number_expression = re.compile("^\d+\s*(-|/)?\s*\d*\s")

    ##regular expression to match longitude/latitude
    lng_lat_expression = re.compile('\(\d+\.')

    ## here, we extract street names from address


    street_name=''
    address_formatted = address.lower().split()
    if len(address_formatted)>0:
        ##check first if address is in the regular format...
        if re.match(address_expression,address.lower()):
            ## get raw address, i.e. 123 xyz st.
            street_address=str(re.match(address_expression,address.lower()).group(0))
            ## get street number
            street_number=str(re.match(street_number_expression,address.lower()).group(0))
            ## get street type i.e. st, ave, ln, etc.
            street_type=str(re.search(street_type_expression,address.lower()).group(0))

            ## remove street number from address
            street_name=street_address.replace(street_number,"")
            ## format and replace street type, i.e. st.=>Street
            new_street_type=clean_street_type(street_type)
            street_name=street_name.replace(street_type,new_street_type)
            street_name=first_upper(street_name)

    return street_name
