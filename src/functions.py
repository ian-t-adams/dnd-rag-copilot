import os
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