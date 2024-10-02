import requests
import os, json

def fetch_json_data(tool_config_id, tool_name):
    url = 'https://portal.cbrain.mcgill.ca/tool_configs/{}/boutiques_descriptor.json'.format(tool_config_id)
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch JSON data. Status code: {response.status_code}")

def generate_rst(json_data, tool_config_id, tool_name):
    # Example processing: create a new rst file using json data
    keys_to_query = ['name', 'description']
    with open('tool_details.rst', 'a') as f:
        initial_text = f"{tool_name} (CBRAIN Tool Config ID: {tool_config_id})\n"
        f.write(initial_text)
        f.write(f"{'='*len(initial_text)}\n\n")
        f.write(".. list-table::\n")
        f.write("   :header-rows: 1\n\n")
        for temp_key in keys_to_query:
            f.write(f"   * - {temp_key}\n")
            f.write(f"     - {json_data[temp_key]}\n")

    return

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
        json_data = fetch_json_data(tool_config_ids[temp_tool], temp_tool)
        generate_rst(json_data, tool_config_ids[temp_tool], temp_tool)

if __name__ == "__main__":
    main()