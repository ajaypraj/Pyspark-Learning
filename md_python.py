def calculate_md5(file_path, block_size=8192):
    md5 = hashlib.md5()
    
    with open(file_path, 'rb') as file:
        while True:
            data = file.read(block_size)
            if not data:
                break
            md5.update(data)

    return md5.hexdigest()
