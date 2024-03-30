def check_if_valid_zip(file):
    
    bash_command = f"""
        if file {file} | grep -q 'zip data'; then
            echo 'File is a valid ZIP archive'
        else
            echo 'File is not a valid ZIP archive'
            exit 1
        fi
    """
    return bash_command