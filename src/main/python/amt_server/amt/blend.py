import json


# Check the basic format of a request
def check_format(json_dict):

    try :
        json_dict = json.loads(json_dict)
        
        # Check configuration
        if not 'configuration' in json_dict :
            return False            
        configuration = json_dict['configuration']
        if not 'type' in configuration :
            return False
        if not 'hit_batch_size' in configuration :
            return False
        if not 'num_assignments' in configuration :
            return False
        if not 'callback_url' in configuration :
            return False
        
        # Check group_id
        if not 'group_id' in json_dict :
            return False
        
        # Check group_context
        if not 'group_context' in json_dict :
            return False
            
        # Check content
        if not 'content' in json_dict :
            return False

        content = json_dict['content']
        point_identifiers = content.keys()
        if len(point_identifiers) == 0 :
            return False
        
    except :
        return False
    
    return True

# Deal with unicode
def convert(input):
    if isinstance(input, dict):
        return {convert(key): convert(value) for key, value in input.iteritems()}
    elif isinstance(input, list):
        return [convert(element) for element in input]
    elif isinstance(input, unicode):
        return input.encode('utf-8')
    else:
        return input
