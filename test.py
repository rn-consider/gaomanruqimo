def getOneNumSum(n):
    sum = 0
    for i in str(n):
        sum +=  int(i)
    return sum
def rank(n,m):
    sums = []
    num_dict = {}
    for i in range(1,int(n)+1):
        num_dict[i] = getOneNumSum(i)
        
    # acc to dict values get a sorted list
    return dict(sorted(num_dict.items(),key = lambda x:x[1]))

        
n = input()
m = input()
sums = rank(n,m)
l = list(sums.keys())
print(l[int(m)-1 ])
