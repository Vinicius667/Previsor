import os
os.system('cls' if os.name == 'nt' else 'clear')
print("#*"*30)
file = os.path.dirname(__file__)
print(file)
os.chdir(file)