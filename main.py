print("hello from git"+"hi")
def email(*args):
    print("Name",args[0])
    print("Age",args[1])

email("Tej","23")    


def my_fun(**myvar):
    print(myvar["name"])
    print(myvar["age"])

my_fun(name='Tej',age=23)    


def my_function(title, *args, **kwargs):
  print("Title:", title)
  print("Positional arguments:", args)
  print("Keyword arguments:", kwargs)

my_function("User Info", "Emil", "Tobias", age = 25, city = "Oslo")

nums = [1,2,2,3,5,5,5]
freq = {}
for n in nums:
    freq[n]=freq.get(n,0)+1

print(freq)    