import requests
import os, json

def fetch_json_data(tool_config_id):
    url = 'https://portal.cbrain.mcgill.ca/tool_configs/{}/boutiques_descriptor.json'.format(tool_config_id)
    response = requests.get(url)
    if response.status_code == 200:
        return response.json(), url
    else:
        raise Exception(f"Failed to fetch JSON data. Status code: {response.status_code}")

def generate_rst(json_data, tool_config_id, tool_name, url):
    # Example processing: create a new rst file using json data
    keys_to_query = ['name', 'description']
    with open('tool_details.rst', 'a') as f:
        initial_text = f"{tool_name} (CBRAIN Tool Config ID: {tool_config_id})\n"
        f.write(initial_text)
        f.write(f"{'-'*len(initial_text)}\n\n")
        f.write('Current boutiques `descriptor <{}>`_ \n'.format(url))
        f.write('************************************\n\n')
        #f.write(".. list-table::\n")
        #f.write("   :header-rows: 1\n\n")
        f.write(f"* **Container**: {json_data["container-image"]["index"] + json_data["container-image"]["image"]}\n")
        if 'url' in json_data.keys():
            f.write(f"* **Documentation**: {json_data['url']}\n")
        for temp_key in keys_to_query:
            f.write(f"* **{temp_key}**: {json_data[temp_key]}\n")
        f.write("\n\n")

        with open('../../external_requirements/{}.json'.format(tool_name), 'r') as f2:
            external_requirements = json.load(f2)
        with open('../../processing_configurations/{}.json'.format(tool_name), 'r') as f3:
            processing_configurations = json.load(f3)

        f.write(".. list-table::\n")
        f.write("   :header-rows: 1\n\n")
        f.write("   * - Argument ID\n")
        f.write("     - Value\n")
        f.write("     - Description\n")
        for temp_key in external_requirements.keys():
            relevant_input = None
            for temp_input in json_data['inputs']:
                if temp_input['id'] == temp_key:
                    relevant_input = temp_input
            if relevant_input is None:
                raise Exception(f"Could not find input with ID {temp_key} in descriptor")
            f.write(f"   * - {temp_key}\n")
            f.write(f"     - {external_requirements[temp_key]}\n")
            f.write(f"     - {relevant_input['description']}\n")

    
    


def main():

    print(os.listdir())
    with open('tool_details.rst', 'w') as f:
        f.write('Tool Details\n')
        f.write('============\n\n')
    with open('tools_to_feature_in_documentation.txt', 'r') as f:
        tools_for_documentation = [line.strip() for line in f]
    print(tools_for_documentation)
    with open('../../tool_config_ids.json') as f:
        tool_config_ids = json.load(f)
    for temp_tool in tools_for_documentation:
        print('Adding {} to documentation'.format(temp_tool))
        json_data, url = fetch_json_data(tool_config_ids[temp_tool])
        generate_rst(json_data, tool_config_ids[temp_tool], temp_tool, url)

if __name__ == "__main__":
    main()