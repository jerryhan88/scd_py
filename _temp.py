import os.path as opath

import pickle

import numpy as np

dist = 0.0
n0, n0XY = 's0', np.array([0.11, 0.20])
for n1, n1XY in [('s1', np.array([0.20, 0.25])),
            ('w0', np.array([0.20, 0.21])),
            ('n1', np.array([0.8, 0.2])),
            ('n2', np.array([0.7, 0.1])),
            ('s2', np.array([0.93, 0.20])),
           ]:
    print('\t %s %s: %f' % (n0, n1, np.linalg.norm(n0XY - n1XY)))
    dist += np.linalg.norm(n0XY - n1XY)
    n0 = n1
    n0XY = n1XY

print(dist)
# assert False

with open(opath.join('_temp', 'prmt_ED_Ex0.pkl'), 'rb') as fp:
    prmt = pickle.load(fp)

H, T, N, K = map(prmt.get, ['H', 'T', 'N', 'K'])
h_i, n_i, w_i = map(prmt.get, ['h_i', 'n_i', 'w_i'])
v_k, R_k = map(prmt.get, ['v_k', 'R_k'])
r_kr, l_kr, u_kr = map(prmt.get, ['r_kr', 'l_kr', 'u_kr'])
C_kr, N_kr, F_kr, p_krij = map(prmt.get, ['C_kr', 'N_kr', 'F_kr', 'p_krij'])
a_i, b_i, c_i, t_ij = map(prmt.get, ['a_i', 'b_i', 'c_i', 't_ij'])
M = len(N) * max(t_ij.values())

with open(opath.join('_temp', 'sol_ED_Ex0_ILP.pkl'), 'rb') as fp:
    sol = pickle.load(fp)

x_krij = sol['x_krij']
a_kri = sol['a_kri']


k, r = 0, 2
krC, krN = C_kr[k, r], N_kr[k, r]

print('Bounds: %f, %f' % (l_kr[k, r], u_kr[k, r]))
_route = {}
print('Route distance')
for j in krN:
    for i in krN:
        if x_krij[k, r, i, j] > 0.5:
            _route[i] = j
            print('\t %s %s: %f' % (i, j, t_ij[i, j]))

o_kr, d_kr = 's0_%d_%d' % (k, r), 's%d_%d_%d' % (len(krC) - 1, k, r)
i = o_kr
route = []
accomplishedTasks = []
while i != d_kr:
    route.append('%s(%.2f)' % (i, a_kri[k, r, i]))
    i = _route[i]

print(route)
print(sum(t_ij[i, j] * x_krij[k, r, i, j] for i in krN for j in krN))