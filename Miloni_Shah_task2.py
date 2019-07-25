from pyspark import SparkContext
from collections import defaultdict
import sys
from copy import deepcopy


class Node():
    def __init__(self,data):
        self.data = data
        self.level = None
        self.parent = []
        self.child = []
        self.isLeaf = True
        self.credit = 0.0
        self.shortestPaths = 1.0

class Tree():
    def __init__(self):
        self.tree = {}


def similar(part):
    for i in part:
        for j in userDict:
            if i != j:
                intersect = userDict[i].intersection(userDict[j])
                if len(intersect)>= threshold:
                    yield (i,j)


def shortestPath(node,nbfs):
    queue = []
    queue.append(nbfs.tree[node])
    while(queue):
        p = queue.pop(0)
        sum_paths = 0.0
        if p.parent:
            for i in p.parent:
                sum_paths = sum_paths+i.shortestPaths
            p.shortestPaths = sum_paths
        for i in p.child:
            queue.append(i)


def BFS(node):
    nodeBFSTree = Tree()
    userBFSTree = Tree()
    queue = []
    x = Node(node)
    x.level = 1
    userBFSTree.tree[x.data] = 1
    nodeBFSTree.tree[x.data] = x
    queue.append(x)
    while queue:
        p = queue.pop(0)
        for i in graph[p.data]:
            if i not in userBFSTree.tree or userBFSTree.tree[i] > p.level:
                p.isLeaf = False
                if i in nodeBFSTree.tree:
                    c = nodeBFSTree.tree[i]
                else:
                    c = Node(i)
                    queue.append(c)
                p.child.append(c)
                c.parent.append(p)
                c.level = c.parent[0].level+1
                userBFSTree.tree[c.data] = c.level
                nodeBFSTree.tree[c.data] = c

    levelDict = defaultdict(list)
    for i in userBFSTree.tree:
        levelDict[userBFSTree.tree[i]].append(i)
    maxLevel = max(levelDict)
    shortestPath(node, nodeBFSTree)
    from_edge = {}
    while maxLevel >= 1:
        nodelist = levelDict[maxLevel]
        for i in nodelist:
            i = nodeBFSTree.tree[i]
            if i.isLeaf is True:
                i.credit = 1.0
                for j in i.parent:
                    from_edge[tuple(sorted((i.data, j.data)))] = i.credit * (j.shortestPaths / i.shortestPaths)
            else:
                toE = []
                for j in i.child:
                    toE.append(from_edge[tuple(sorted((j.data, i.data)))])
                i.credit = 1 + sum(toE)
                for j in i.parent:
                    from_edge[tuple(sorted((i.data, j.data)))] = i.credit * (j.shortestPaths / i.shortestPaths)
        maxLevel = maxLevel - 1
    return from_edge


def calc_betweenness(iterator):
    for i in iterator:
        x = BFS(i)
        for j in x:
            yield (j,x[j])


def modularity():
    q = 0
    for i in v:
        if len(visitedDict) == len(v):
            break
        if i not in visitedDict:
            vis = {}
            queue = []
            queue.append(i)
            vis[i] = True
            visitedDict[i] = True
            while queue:
                s = queue.pop(0)
                for j in graph[s]:
                    if j not in vis:
                        queue.append(j)
                        vis[j] = True
                        visitedDict[j] = True
            checkDict = {}
            for j in vis:
                for k in vis:
                    if j!=k:
                        tup = tuple(sorted((j,k)))
                        if tup not in checkDict:
                            checkDict[tup] = 0
                            if tup in eDict:
                                q = q + 1 - degreeDict[j]*degreeDict[k]/(2*m)
                            else:
                                q = q + 0 - degreeDict[j]*degreeDict[k]/(2*m)
    return q


def find_Communities():
    communities = []
    for i in v:
        if len(visitedDict) == len(v):
            break
        if i not in visitedDict:
            vis = {}
            queue = []
            queue.append(i)
            vis[i] = True
            visitedDict[i] = True
            while queue:
                s = queue.pop(0)
                for j in maxGraph[s]:
                    if j not in vis:
                        queue.append(j)
                        vis[j] = True
                        visitedDict[j] = True
            communities.append(vis)
    return communities


sc = SparkContext()
threshold = int(sys.argv[1])
ipfile = sys.argv[2]
inputRDD = sc.textFile(ipfile).map(lambda x: x.split(',')).filter(lambda x: x[0]!='user_id')

userDict = inputRDD.groupByKey().mapValues(set).collectAsMap()
v = inputRDD.map(lambda x:x[0]).distinct()
edges = v.mapPartitions(similar).persist()
eDict = edges.map(lambda x:(x,0)).collectAsMap()
m = edges.map(lambda x: tuple(sorted(x))).distinct().count()
vertices = edges.map(lambda x:x[0]).distinct()
v = vertices.collect()
graph = defaultdict(list)
e1 = edges.collect()
for ee in e1:
    graph[ee[0]].append(ee[1])


degreeDict = {}
for i in graph:
    degreeDict[i] = len(graph[i])

visitedDict = {}
q = modularity()/(2*m)
max_mod = q
flag = True
count_of_edges = m
maxGraph = deepcopy(graph)
out1 = sys.argv[3]

while count_of_edges > 0:
    betweenness = vertices.mapPartitions(calc_betweenness).reduceByKey(lambda x, y: x+y)
    betweenness = betweenness.map(lambda x:(x[0], x[1]/2)).sortBy(lambda x: (-x[1], x[0]))
    if flag == True:
        flag = False
        f1 = open(out1,'w+')
        for i in betweenness.collect():
            f1.write('(\''+i[0][0]+'\', \''+i[0][1]+'\'), '+str(i[1])+'\n')
        f1.close()
    maxB = betweenness.first()
    i = maxB[0][0]
    j = maxB[0][1]
    graph[i].remove(j)
    graph[j].remove(i)
    visitedDict = {}
    q = modularity()/(2*m)
    if q> max_mod:
        max_mod = q
        maxGraph = deepcopy(graph)
    count_of_edges = count_of_edges -1

visitedDict = {}
communities = find_Communities()
result = sc.parallelize(communities).map(lambda x: tuple(sorted(x))).sortBy(lambda x: (len(x),x[0])).collect()
out2 = sys.argv[4]
# = "Miloni_Shah_community.txt"
f2 = open(out2,'w+')
for i in result:
    for j in i:
        if j!= i[-1]:
            f2.write('\''+j+'\', ')
        else:
            f2.write('\''+j+'\'')
    f2.write('\n')
f2.close()

