
def extract_subfields(selection):
    subfield = {selection.name: []}
    for sub_selection in selection.selections:
        if sub_selection.selections:
            subfield[selection.name].append(extract_subfields(sub_selection))
        else: 
            subfield[selection.name].append(sub_selection.name)
             
    return subfield


def extract_fields(query_fields):
    fields = []
    
    for field in query_fields.selected_fields:
        for selection in field.selections:
            if selection.selections:
                subfield = extract_subfields(selection)
                fields.append(subfield)
            else: 
                fields.append(selection.name)
                
    return fields
