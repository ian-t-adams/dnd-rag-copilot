import os
import json
from urllib.parse import urlparse

def blob_name_and_url_dict(blob_service_client, container_name) -> dict:
    """
    This function takes in a blob service client and a container name and returns
    a dictionary containing the blob details indexed by the container name, file type, and then file name or url.
    :param blob_service_client: BlobServiceClient object
    :param container_name: Name of the container to be accessed
    :return: Dictionary containing the blob details indexed by the container name, file type, and then file name or url
    """
    # Create an empty dictionary to store the blob details
    blob_name_and_url_dict = {}

    try:
        # Create the BlobContainerClien object so we can connect to the blob storage container based on the container name
        container_client = blob_service_client.get_container_client(container_name)
        print(f"The container {container_name} is being accessed.")
    except Exception as e:
        print(f"ERROR: Could not access the container {container_name}.")
        print(e)
    
    try:
        # List all blobs in the container
        blob_list = container_client.list_blobs()

        # If the container name is not in the dictionary, add it
        if container_name not in blob_name_and_url_dict:
            blob_name_and_url_dict[container_name] = {}

        # Loop through each blob in the container
        for blob in blob_list:
            blob_client = container_client.get_blob_client(blob)
            print(f"The file {blob.name} located at {blob_client.url} is being added to the blob list.")
            
            # Get the file extension
            _, file_extension = os.path.splitext(blob.name)

            # Now you can use file_extension in your dictionary
            blob_file_dict = {
                'file_name': blob.name,
                'blob_url': blob_client.url
            }

            # If the file type is not in the dictionary, add it
            if file_extension not in blob_name_and_url_dict[container_name]:
                blob_name_and_url_dict[container_name][file_extension] = {}

            # Add the dictionary to the dictionary associated with the file type
            blob_name_and_url_dict[container_name][file_extension][blob.name] = blob_file_dict

        return blob_name_and_url_dict
    except Exception as e:
        print(f"ERROR: Could not list the blobs in the container {container_name}.")
        print(e)


def doc_intel_pdf(document_analysis_client, doc_intel_model, file_name, blob_url, poller_result_or_dict_flag) -> dict:
    '''
    Function to analyze a PDF document using the Document Intelligence service based
    on the url of the blob containing the document - this is the raw document.
    The url must contain a SAS token with read permissions.

    :param document_analysis_client: DocumentAnalysisClient object
    :param doc_intel_model: Document Intelligence model to use
    :param file_name: Name of the file to be analyzed
    :param blob_url: URL of the blob to be analyzed
    :param poller_result_or_dict_flag: Flag to return the poller result ('result')  or the dictionary ('dict) or both ('both)
    :return: Document Intelligence results as the result object, a dictionary, or both
    '''
    try:        
        # Extract the blob name from the blob URL
        blob_name = urlparse(blob_url).path.split('/')[-1]
        
        _, file_extension = os.path.splitext(blob_name)    
        if file_extension == '.pdf':
            # Analyze the document using the Document Intelligence service
            doc_intel_pdf_poller = document_analysis_client.begin_analyze_document_from_url(
                                            doc_intel_model, 
                                            document_url=blob_url
                                            )
            doc_intel_pdf_result = doc_intel_pdf_poller.result()
            doc_intel_pdf_result_dict = doc_intel_pdf_result.to_dict()

            print(f'File {file_name} was analyzed using the Document Intelligence service.')
            
            # Return the result based on the flag poller_result_or_dict_flag
            if poller_result_or_dict_flag == 'result':
                return doc_intel_pdf_result
            if poller_result_or_dict_flag == 'dict':
                return doc_intel_pdf_result_dict
            if poller_result_or_dict_flag == 'both':
                return doc_intel_pdf_result, doc_intel_pdf_result_dict

        else:
            print(f'File {file_name} is not a PDF according to the file extension.')
            return None

    except Exception as e:
        print(f'Error: {e}')
        return None


def local_file_write(data, text_or_json_flag=str, file_path=str, file_name_with_extension=str) -> None:
    '''
    :param raw_result: the raw result from the document intelligence service
    :param result_dict: the dictionary of results from the document intelligence service
    :param text_or_json_flag: a flag to determine if the file should be written as a text file or a json file
    :return: None
    '''
    try:
        if text_or_json_flag == 'text':
            with open(file_path + '/' + file_name_with_extension, 'w', encoding='utf-8') as f:
                f.write(str(data))
                print('File successfully written to ' + file_path + '/' + file_name_with_extension)
        elif text_or_json_flag == 'json':
            with open(file_path + '/' + file_name_with_extension, 'w') as f:
                json.dump(data, f)
                print('File successfully written to ' + file_path + '/' + file_name_with_extension)
        else:
            print('''Please specify if you want to write a text or json file by setting the text_or_json_flag parameter to 'json' or 'text'.''')

    except Exception as e:
        print(e)
    
    return None

def local_file_read(file_path: str, text_or_json_flag: str):
    '''
    :param file_path: the path to the file to read
    :param text_or_json_flag: a flag to determine if the file is a text file or a json file
    :return: the contents of the file as either a dictionary or raw text
    '''
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            if text_or_json_flag == 'text':
                return f.read()
            elif text_or_json_flag == 'json':
                return json.load(f)
            else:
                print('''Please specify if you want to read a text or json file by setting the text_or_json_flag parameter to 'json' or 'text'.''')
                return None
    except Exception as e:
        print(e)
        return None


def write_to_blob(object_to_upload, blob_service_client, container_name=str, virtual_directory_name=str, file_name=str, json_dump_flag=bool):
    '''
    This function will write a file to a blob storage container; uses json.dumps to convert to json if json_dump_flag is True.
    :param object_to_upload: The object to upload to the blob storage container
    :param blob_service_client: The blob service client object; raw_blob_service_client, processed_blob_service_client, or final_blob_service_client in this exercise
    :param container_name: The name of the container to upload to; raw_container_name, processed_container_name, or final_container_name in this exercise
    :param virtual_directory_name: The name of the virtual directory to upload to; raw_results or dictionaries in this exercise
    :param file_name: The name of the file to upload, string format
    :param json_dump_flag: A boolean flag to indicate if the object should be converted to json before uploading
    '''
    blob_name = str(container_name + '/' + virtual_directory_name + '/' + file_name)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    print(blob_client) # Only needed for debugging
    if json_dump_flag == True:
        try:
            json_object_to_upload = json.dumps(object_to_upload)
            blob_client.upload_blob(json_object_to_upload, blob_type="BlockBlob")
            print(f'Uploaded {file_name} to {blob_name}')
        except Exception as e:
            print(f'Error uploading {file_name} to {blob_name} with the following Error: {e}')
    
    else:
        try:
            str_object_to_upload = str(object_to_upload)
            blob_client.upload_blob(str_object_to_upload, blob_type="BlockBlob")
            print(f'Uploaded {file_name} to {blob_name}')
        except Exception as e:
            print(f'Error uploading {file_name} to {blob_name} with the following Error: {e}')

