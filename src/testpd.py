
from sortedcontainers import SortedList

class A:
    def __init__(self,x):
        self.x = x

    def __lt__(self,other):
        return self.x < other.x

    def __eq__(self,other):
        return self.x == other.x

if __name__ == "__main__":
    null = A(0)
    a = A(1)
    b = A(2)
    c = A(3)
    l = SortedList([null,a,b,c])
    print([y.x for y in l])
    b.x = 4
    c.x = -1
    l.update([c])

    print([y.x for y in l])

