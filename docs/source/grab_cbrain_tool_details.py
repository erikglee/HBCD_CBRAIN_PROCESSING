import requests
import os, json, re

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
        initial_text = f"{tool_name} (`CBRAIN Tool Config ID: {tool_config_id} <{url}>`_)\n"
        f.write(initial_text)
        f.write(f"{'-'*len(initial_text)}\n\n")
        #f.write('Current boutiques `descriptor <{}>`_ \n'.format(url))
        #f.write('************************************\n\n')
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

        f.write("External Requirements\n")
        f.write("*********************\n\n")
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
            description = relevant_input['description'].replace('\n', ' ').replace('\r', '')
            f.write(f"   * - {temp_key}\n")
            f.write(f"     - {external_requirements[temp_key]}\n")
            f.write(f"     - {description}\n")
        f.write("\n\n")

        f.write("Other Processing Settings\n")
        f.write("*************************\n\n")
        f.write(".. list-table::\n")
        f.write("   :header-rows: 1\n\n")
        f.write("   * - Argument ID\n")
        f.write("     - Value\n")
        f.write("     - Description\n")
        for temp_key in processing_configurations.keys():
            relevant_input = None
            for temp_input in json_data['inputs']:
                if temp_input['id'] == temp_key:
                    relevant_input = temp_input
                    description = relevant_input['description'].replace('\n', ' ').replace('\r', '')
            if relevant_input is None:
                raise Exception(f"Could not find input with ID {temp_key} in descriptor")
            f.write(f"   * - {temp_key}\n")
            f.write(f"     - {processing_configurations[temp_key]}\n")
            f.write(f"     - {description}\n")
        f.write("\n\n\n\n")

        f.write("Pipeline Outputs\n")
        f.write("*************************\n\n")
        f.write(".. list-table::\n")
        f.write("   :header-rows: 1\n\n")
        f.write("   * - ID\n")
        f.write("     - Path Relative to Working Directory\n")
        f.write("     - Path in Output DataProvider\n")
        f.write("     - Description\n")
        for temp_output in json_data['output-files']:
            relevant_input = None
            for temp_input in json_data['inputs']:
                if temp_input['id'] == temp_key:
                    relevant_input = temp_input
                    description = relevant_input['description'].replace('\n', ' ').replace('\r', '')
            if relevant_input is None:
                raise Exception(f"Could not find input with ID {temp_key} in descriptor")
            f.write(f"   * - {temp_output['id']}\n")
            f.write(f"     - {temp_output['path-template']}\n")
            try:
                output_path = json_data["custom"]["cbrain:integrator_modules"]["BoutiquesForcedOutputBrowsePath"][temp_output['id']]
                f.write(f"     - {output_path}\n")
            except:
                f.write(f"     - NA\n")
            f.write(f"     - {temp_output['description']}\n")
        
        comp_proc_recs_dir = '../../comprehensive_processing_prerequisites'
        requirements_files = []
        for filename in os.listdir(comp_proc_recs_dir):
            # Use regular expression to match pipeline name with or without underscore and number
            match = re.match(rf"^{tool_name}(?:_[0-9]+)?\.json$", filename)
            if match:
                requirements_files.append(os.path.join(comp_proc_recs_dir, filename))
        requirements_files.sort()
        if len(requirements_files) > 0:
            is_qc_used = False
            f.write("File Selection For Processing\n")
            f.write("*****************************\n\n")
            f.write("The data for one imaging session (i.e. sub-1, ses-V02) must\n")
            f.write("satisfy at least one of the following requirements for processing.\n")
            f.write("If at least one requirement is satisfied processing can occur.\n")
            f.write("If processing can occur, any files that match any requirements \n")
            f.write("will be considered for processing.\n\n")
            for filename in requirements_files:
                with open(filename, 'r') as f2:
                    temp_processing_reqs = json.load(f2)
                requirement_name = 'Requirement Group: ' + os.path.basename(filename).replace('.json', '')
                f.write(f"{requirement_name}\n")
                f.write(f"{'~'*len(os.path.basename(filename))}\n\n")
                f.write(".. list-table::\n")
                f.write("   :header-rows: 1\n\n")
                f.write("   * - Group Name\n")
                f.write("     - Term\n")
                f.write("     - Included (True)/Excluded (False)\n")
                for temp_key in temp_processing_reqs.keys():
                    file_naming_dict = temp_processing_reqs[temp_key]['file_naming']
                    if 'qc_criteria' in temp_processing_reqs[temp_key].keys():
                        is_qc_used = True
                    for i, temp_inner_key in enumerate(file_naming_dict.keys()):
                        if i == 0:
                            f.write(f"   * - {temp_key}\n")
                        else:
                            f.write(f"   * -   \n")
                        f.write(f"     - {temp_inner_key}\n")
                        f.write(f"     - {file_naming_dict[temp_inner_key]}\n")



                f.write("\n\n")
            if is_qc_used:
                f.write("Quality Control Selection Information\n")
                f.write("**************************************\n\n")
                f.write("")
                for temp_key in temp_processing_reqs.keys():
                    if 'qc_criteria' in temp_processing_reqs[temp_key].keys():
                        f.write(f"**{temp_key}**\n")
                        f.write(f"{'~'*len(temp_key)}\n\n")
                        if "num_to_keep" in temp_processing_reqs[temp_key].keys():
                            f.write("Processing will look for best {} file(s) to keep using the following criteria.".format(temp_processing_reqs[temp_key]["num_to_keep"]))
                        else:
                            f.write("Processing will include all files passing the following criteria.")
                        outer_qc_list = temp_processing_reqs[temp_key]['qc_criteria']
                        for inner_qc_list in outer_qc_list:
                            f.write(".. list-table::\n")
                            f.write("   :header-rows: 1\n\n")
                            f.write("   * - scans.tsv Field\n")
                            f.write("     - Value\n")
                            f.write("     - Operator\n")
                            for temp_qc in inner_qc_list:
                                key = next(iter(temp_qc))
                                f.write(f"   * - {key}\n")
                                f.write(f"     - {temp_qc[key][0]}\n")
                                f.write(f"     - {temp_qc[key][1]}\n")
                            f.write("\n\n\n")


    
    


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