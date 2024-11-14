def onBarUpdate(bars):
    print(f"plot size:{len(bars)}")


list1 = [1, 2, 3]
list2 = [4, 5, 6]

# 直接赋值
list1[len(list1):] = list2
print(list1)  # 输出：[1, 2, 3, 4, 5, 6]

list2.append(7)
print(list2)
print(list1)

print('hello world')