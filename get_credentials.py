def get_credentials():
    with open('credentials.txt', 'r') as file:
        arr = []
        for line in file:
            arr.append(line.strip())
    return (arr[0], arr[1], arr[2], arr[3], arr[4])