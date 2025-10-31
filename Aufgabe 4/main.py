from sparkstart import scon
import os
import matplotlib.pyplot as plt

#--------------------------------Aufgabe a----------------------------------
print("\nAufgabe a)")
mylist = list(range(1,101))
rd1 = scon.parallelize(mylist)

mylist = list(range(50,151))
rd2 = scon.parallelize(mylist)

print(rd1.takeOrdered(22))
print(rd2.takeOrdered(101))

rd1.count()
rd2.count()

rd1.first()
rd2.first()

rd1.takeSample(False,10)
rd2.takeSample(False,10)


#--------------------------------Aufgabe b----------------------------------
print("\nAufgabe b)")
rd1_div_5_and_7 = rd1.filter(lambda x: (x % 5 == 0) and (x % 7 == 0))
print(rd1_div_5_and_7.takeOrdered(100))


#--------------------------------Aufgabe c----------------------------------
print("\nAufgabe c)")
intersection = rd1.intersection(rd2).sortBy(lambda x: x)
union_set = rd1.union(rd2).distinct().sortBy(lambda x: x)
diff_rd1_minus_rd2 = rd1.subtract(rd2).sortBy(lambda x: x)
diff_rd2_minus_rd1 = rd2.subtract(rd1).sortBy(lambda x: x)
cartesian_product = rd1.cartesian(rd2).sortBy(lambda p: (p[0], p[1]))

print("\nDurchschnittsmenge:")
intersection_vals = intersection.collect()
print(intersection_vals)
print("Anzahl:", len(intersection_vals))

print("\nVereinigungsmenge:")
union_vals = union_set.collect()
print(union_vals)
print("Anzahl:", len(union_vals))

print("\nDifferenzmenge:")
diff1_vals = diff_rd1_minus_rd2.collect()
print(diff1_vals)
print("Anzahl:", len(diff1_vals))

print("\nDifferenzmenge:")
diff2_vals = diff_rd2_minus_rd1.collect()
print(diff2_vals)
print("Anzahl:", len(diff2_vals))

print("\nKreuzprodukt:")
cart_vals = cartesian_product.collect()
print(cart_vals)
print("Anzahl:", len(cart_vals))


#--------------------------------Aufgabe d----------------------------------
rd3 = rd1.map(lambda x: (x, 1.0 / x))
sum_rd3 = rd3.map(lambda kv: kv[1]).sum()
print("\nSumme:", sum_rd3)

rd3_sorted = rd3.sortByKey().collect()
xs = [x for x, _ in rd3_sorted]
ys = [y for _, y in rd3_sorted]

plt.figure(figsize=(8, 4))
plt.plot(xs, ys, marker='o', linestyle='-', color='tab:blue')
plt.title('1/x über x (rd1)')
plt.xlabel('x')
plt.ylabel('1/x')
plt.grid(True, alpha=0.3)

plt.tight_layout()
plt.close()

#--------------------------------Aufgabe e----------------------------------
product_rd2 = rd2.fold(1, lambda a, b: a * b)
print("\nAufgabe e)")
print(product_rd2)

scon.stop()
