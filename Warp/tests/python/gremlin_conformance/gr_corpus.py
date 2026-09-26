"""The Gremlin conformance corpus: hand-written traversals over the standard 'modern' and 'classic' toy graphs plus seeded random
traversals. Each Case is a list of scripts run in order against one graph; the last answer (and, for `bc` cases, the same traversal
sent as bytecode) is compared with the recorded answers of a real Apache TinkerPop Gremlin Server (see gr_harness.py)."""
import random

# ---------------------------------------------------------------------------------------------------------------- toy graphs
MODERN = [
    "g.V().drop().iterate()",
    "g.addV('person').property(T.id,1L).property('name','marko').property('age',29).next()",
    "g.addV('person').property(T.id,2L).property('name','vadas').property('age',27).next()",
    "g.addV('software').property(T.id,3L).property('name','lop').property('lang','java').next()",
    "g.addV('person').property(T.id,4L).property('name','josh').property('age',32).next()",
    "g.addV('software').property(T.id,5L).property('name','ripple').property('lang','java').next()",
    "g.addV('person').property(T.id,6L).property('name','peter').property('age',35).next()",
    "g.V(1L).addE('knows').to(__.V(2L)).property(T.id,7L).property('weight',0.5d).iterate()",
    "g.V(1L).addE('knows').to(__.V(4L)).property(T.id,8L).property('weight',1.0d).iterate()",
    "g.V(1L).addE('created').to(__.V(3L)).property(T.id,9L).property('weight',0.4d).iterate()",
    "g.V(4L).addE('created').to(__.V(5L)).property(T.id,10L).property('weight',1.0d).iterate()",
    "g.V(4L).addE('created').to(__.V(3L)).property(T.id,11L).property('weight',0.4d).iterate()",
    "g.V(6L).addE('created').to(__.V(3L)).property(T.id,12L).property('weight',0.2d).iterate()",
]
CLASSIC = [
    "g.V().drop().iterate()",
    "g.addV().property(T.id,1L).property('name','marko').property('age',29).next()",
    "g.addV().property(T.id,2L).property('name','vadas').property('age',27).next()",
    "g.addV().property(T.id,3L).property('name','lop').property('lang','java').next()",
    "g.addV().property(T.id,4L).property('name','josh').property('age',32).next()",
    "g.addV().property(T.id,5L).property('name','ripple').property('lang','java').next()",
    "g.addV().property(T.id,6L).property('name','peter').property('age',35).next()",
    "g.V(1L).addE('knows').to(__.V(2L)).property(T.id,7L).property('weight',0.5f).iterate()",
    "g.V(1L).addE('knows').to(__.V(4L)).property(T.id,8L).property('weight',1.0f).iterate()",
    "g.V(1L).addE('created').to(__.V(3L)).property(T.id,9L).property('weight',0.4f).iterate()",
    "g.V(4L).addE('created').to(__.V(5L)).property(T.id,10L).property('weight',1.0f).iterate()",
    "g.V(4L).addE('created').to(__.V(3L)).property(T.id,11L).property('weight',0.4f).iterate()",
    "g.V(6L).addE('created').to(__.V(3L)).property(T.id,12L).property('weight',0.2f).iterate()",
]
EMPTY = ["g.V().drop().iterate()"]
GRAPHS = {"modern": MODERN, "classic": CLASSIC, "empty": EMPTY}


class Case:
    def __init__(self, name, steps, graph="modern", ordered=None, bc=True, mutating=False, batch=None, msg=False):
        self.name = name
        self.steps = steps if isinstance(steps, list) else [steps]
        self.graph = graph
        self.script = self.steps[-1]
        self.ordered = ordered if ordered is not None else ("order(" in self.script and "groupCount" not in self.script)
        self.bc = bc and len(self.steps) == 1
        self.mutating = mutating or any(m in st for st in self.steps for m in (".property(", ".addV(", ".addE(", ".drop(", "mergeV(", "mergeE(")) and graph != "empty"
        self.batch = batch
        self.msg = msg


CASES = []
_counters = {}


def add(group, scripts, **kw):
    if isinstance(scripts, str):
        scripts = [scripts]
    for s in scripts:
        n = _counters.get(group, 0) + 1
        _counters[group] = n
        CASES.append(Case(f"{group}_{n:03d}", s, **kw))


# ---------------------------------------------------------------------------------------------------------------- sources / filters
add("src", """g.V()
g.V(1)
g.V(1,2)
g.V([1,2])
g.V(99)
g.V(1L,2L,3L)
g.E()
g.E(7)
g.E(7,8)
g.E(99)
g.V().count()
g.E().count()
g.V().id()
g.E().id()
g.V().label()
g.E().label()
g.inject(1,2,3)
g.inject('a','b')
g.inject([1,2],[3])
g.V().inject(0).count()
g.V().hasLabel('person').count()
g.V().hasLabel('person','software')
g.V().hasLabel(within('person'))
g.V().hasLabel('nothing')
g.E().hasLabel('knows')
g.V().has('name')
g.V().has('name','marko')
g.V().has('name','nobody')
g.V().has('age',29)
g.V().has('age',gt(28))
g.V().has('age',gte(29))
g.V().has('age',lt(29))
g.V().has('age',lte(29))
g.V().has('age',neq(29))
g.V().has('age',eq(29))
g.V().has('age',between(27,32))
g.V().has('age',inside(27,35))
g.V().has('age',outside(28,33))
g.V().has('age',within(27,29))
g.V().has('age',without(27,29))
g.V().has('age',within([27,29]))
g.V().has('name',within('marko','vadas'))
g.V().has('person','name','marko')
g.V().has('software','name','lop')
g.V().has(label,'person')
g.V().has(label,within('person','software'))
g.V().has(id,1)
g.V().has(id,within(1,2))
g.V().hasNot('age')
g.V().hasNot('name')
g.V().hasId(1)
g.V().hasId(1,2)
g.V().hasId(within(1,2))
g.V().has('name',containing('a'))
g.V().has('name',startingWith('m'))
g.V().has('name',endingWith('o'))
g.V().has('name',notContaining('a'))
g.V().has('name',notStartingWith('m'))
g.V().has('name',notEndingWith('o'))
g.V().has('name',regex('m.*'))
g.V().has('name',regex('^ma.*o$'))
g.V().has('name',regex('ma'))
g.V().has('name',notRegex('m.*'))
g.V().has('age',gt(27).and(lt(35)))
g.V().has('age',gt(30).or(lt(28)))
g.V().has('age',gt(27).and(lt(35)).and(neq(29)))
g.V().has('lang','java')
g.V().has('lang')
g.V().has('age',gt(100))
g.V().has('weight')
g.E().has('weight',gt(0.5d))
g.E().has('weight',0.4d)
g.E().has('weight',lt(0.5d))
g.E().has('weight',within(0.2d,0.5d))
g.E().has('weight')
g.E().hasLabel('created').has('weight',gte(0.4d))
g.V().and(has('age'),has('name'))
g.V().or(has('lang'),has('age',gt(30)))
g.V().not(has('age'))
g.V().not(out())
g.V().where(out())
g.V().where(__.in())
g.V().where(out().count().is(gt(1)))
g.V().where(out('created'))
g.V().filter(out('created'))
g.V().filter(has('age',gt(30)))
g.V().values('age').is(29)
g.V().values('age').is(gt(30))
g.V().values('age').is(within(27,29))
g.V().values('age').is(inside(28,33))
g.V().properties().hasKey('name')
g.V().properties().hasKey('name','age')
g.V().properties().hasValue('marko')
g.V().properties().hasValue(29)
g.V().properties('name').hasValue(containing('a'))
g.V().properties().hasLabel('age')
g.V().hasLabel('person').has('age',gt(28)).has('name',startingWith('j'))
g.V().has('age',gt(28)).hasLabel('person').values('name')
g.V().coin(1.0d).count()
g.V().coin(0.0d).count()
g.V().hasLabel('person').where(values('age').is(gt(30)))
g.V().where(values('age').is(lt(30)))
g.V().where(has('age',gt(30)))
g.V().hasLabel('software').where(__.in('created').count().is(gt(1)))""".split("\n"))

# ---------------------------------------------------------------------------------------------------------------- navigation
add("nav", """g.V(1).out()
g.V(1).out('knows')
g.V(1).out('knows','created')
g.V(1).out('nothing')
g.V(1).in()
g.V(3).in()
g.V(3).in('created')
g.V(3).in('knows')
g.V(1).both()
g.V(4).both()
g.V(4).both('knows')
g.V(4).both('created')
g.V(1).outE()
g.V(1).outE('knows')
g.V(3).inE()
g.V(3).inE('created')
g.V(4).bothE()
g.V(4).bothE('created')
g.V(1).outE().inV()
g.V(1).outE().outV()
g.V(1).outE().otherV()
g.V(3).inE().outV()
g.V(3).inE().otherV()
g.V(3).bothE().otherV()
g.V(4).bothE().otherV()
g.V(2).bothE().bothV()
g.V().out().out()
g.V().out().out().values('name')
g.V().out().in()
g.V().both().both().count()
g.V(1).out().out().path()
g.V(1).out().out().path().by('name')
g.V().outE().count()
g.V().inE().count()
g.V().bothE().count()
g.V().out().count()
g.V().out('created').values('name')
g.V().out('created').dedup().values('name')
g.V().out('knows').out('created').values('name')
g.V(1).out('knows').has('age',gt(30)).values('name')
g.V().hasLabel('person').out('created').hasLabel('software').values('name')
g.E().outV()
g.E().inV()
g.E().bothV()
g.E().outV().values('name')
g.E().inV().values('name')
g.E().hasLabel('knows').values('weight')
g.E().hasLabel('knows').inV().values('name')
g.E().has('weight',gt(0.5d)).outV().values('name')
g.E(7).outV()
g.E(7).inV()
g.E(7).bothV()
g.V(1).outE().inV().outE().inV()
g.V().hasLabel('software').in('created').values('name')
g.V().hasLabel('software').in('created').dedup().values('name')
g.V(3).in('created').out('created').values('name')
g.V(3).in('created').out('created').dedup().values('name')
g.V(3).in('created').out('created').simplePath().values('name')
g.V(1).repeat(out()).times(2).path()
g.V(1).as('a').out().as('b').out().as('c').select('a','b','c')
g.V().as('a').out('created').as('b').select('a','b').by('name')
g.V().hasLabel('person').as('a').out('knows').as('b').select('a','b').by('name')
g.V(1).out().out().count()
g.V(1).out().out().out().count()
g.V().out().out().out().count()""".split("\n"))

# ---------------------------------------------------------------------------------------------------------------- properties
add("prop", """g.V().values('name')
g.V().values('name','age')
g.V().values()
g.V().values('nothing')
g.V().properties()
g.V().properties('name')
g.V().properties('name','age')
g.V().properties('name').value()
g.V().properties('name').key()
g.V().properties().key()
g.V().properties().value()
g.V().properties().label()
g.V().valueMap()
g.V().valueMap('name')
g.V().valueMap('name','age')
g.V().valueMap(true)
g.V().valueMap(true,'name')
g.V().valueMap(false)
g.V().valueMap().with(WithOptions.tokens)
g.V().valueMap('name').with(WithOptions.tokens)
g.V().elementMap()
g.V().elementMap('name')
g.V().elementMap('name','age')
g.E().valueMap()
g.E().valueMap('weight')
g.E().valueMap(true)
g.E().elementMap()
g.E().elementMap('weight')
g.E().properties()
g.E().properties('weight')
g.E().properties('weight').value()
g.E().values('weight')
g.E().values()
g.V(1).valueMap()
g.V(1).properties('name').valueMap()
g.V().propertyMap('name')
g.V().hasLabel('person').values('age').sum()
g.V().hasLabel('person').values('age').min()
g.V().hasLabel('person').values('age').max()
g.V().hasLabel('person').values('age').mean()
g.V().hasLabel('person').values('age').count()
g.V().hasLabel('person').values('age').fold()
g.V().values('age').fold().unfold()
g.V().values('age').sum()
g.E().values('weight').sum()
g.E().values('weight').mean()
g.E().values('weight').max()
g.E().values('weight').min()
g.V().values('name').fold()
g.V().values('name').max()
g.V().values('name').min()
g.V().values('nothing').sum()
g.V().values('nothing').max()
g.V().values('nothing').mean()
g.V().values('nothing').count()
g.V().values('nothing').fold()
g.V().id().sum()
g.V().values('age').map(constant(1)).sum()
g.V().hasLabel('person').values('age').sum().math('_ / 4')
g.V().values('age').math('_ * 2')
g.V().values('age').math('_ + 1').by()
g.E().values('weight').math('_ * 10')
g.V().as('a').values('age').as('b').math('a + b').by(id).by()
g.V().values('age').math('sqrt(_)')
g.V().values('age').math('abs(0 - _)')
g.V().values('age').math('_ ^ 2')""".split("\n"))

# ---------------------------------------------------------------------------------------------------------------- ordering / paging
add("ord", """g.V().order().by('name')
g.V().order().by('name',desc)
g.V().hasLabel('person').order().by('age')
g.V().hasLabel('person').order().by('age',desc)
g.V().hasLabel('person').order().by('age',desc).by('name')
g.V().hasLabel('person').order().by('age').values('name')
g.V().hasLabel('person').order().by(values('age'),desc)
g.V().hasLabel('person').order().by(out().count(),desc).by('name')
g.V().order().by(id,desc)
g.V().order().by(label).by('name')
g.V().hasLabel('person').values('age').order()
g.V().hasLabel('person').values('age').order().by(desc)
g.V().hasLabel('person').values('name').order()
g.V().hasLabel('person').values('name').order().by(desc)
g.V().values('name').order().by(shuffle).count()
g.E().order().by('weight')
g.E().order().by('weight',desc).by(id)
g.E().order().by('weight',desc).limit(3)
g.V().order().by(id).limit(2)
g.V().order().by(id).range(1,3)
g.V().order().by(id).skip(4)
g.V().order().by(id).tail()
g.V().order().by(id).tail(2)
g.V().order().by(id).limit(0)
g.V().order().by(id).range(2,-1)
g.V().order().by(id).range(2,10)
g.V().hasLabel('person').order().by('age').limit(1).values('name')
g.V().values('name').order().limit(2)
g.V().values('name').order().tail(2)
g.V().values('age').order().by(desc).limit(2)
g.V().limit(0)
g.V().limit(100).count()
g.V().range(0,2).count()
g.V().skip(2).count()
g.V().skip(100).count()
g.V().tail().count()
g.V().hasLabel('person').fold().order(local).by('age')
g.V().hasLabel('person').values('age').fold().order(local)
g.V().hasLabel('person').values('age').fold().order(local).by(desc)
g.V().hasLabel('person').values('age').fold().limit(local,2)
g.V().hasLabel('person').values('age').fold().range(local,1,3)
g.V().hasLabel('person').values('age').fold().tail(local,1)
g.V().hasLabel('person').values('age').fold().tail(local,2)
g.V().hasLabel('person').values('age').fold().skip(local,1)
g.V().hasLabel('person').values('age').order().fold().count(local)
g.V().values('age').fold().sum(local)
g.V().values('age').fold().min(local)
g.V().values('age').fold().max(local)
g.V().values('age').fold().mean(local)
g.V().values('age').fold().dedup(local)
g.V().values('lang').fold().dedup(local)
g.V().values('age').dedup().count()
g.V().values('lang').dedup()
g.V().hasLabel('person','software').label().dedup()
g.V().out().dedup().count()
g.V().out().dedup().values('name')
g.V().as('a').out().as('b').dedup('a','b').count()
g.V().as('a').out().dedup('a').select('a').values('name')
g.V().hasLabel('person').groupCount().by('age').unfold().order().by(keys)
g.V().groupCount().by(label).unfold().order().by(values,desc)""".split("\n"))

# ---------------------------------------------------------------------------------------------------------------- grouping / projection
add("grp", """g.V().group().by(label)
g.V().group().by(label).by('name')
g.V().group().by(label).by(count())
g.V().group().by('age')
g.V().group().by(label).by(values('age').sum())
g.V().group().by(label).by(values('age').mean())
g.V().group().by(label).by(values('name').fold())
g.V().hasLabel('person').group().by('age').by('name')
g.V().hasLabel('person').group().by(out().count()).by('name')
g.V().group().by(label).by(out().count())
g.V().group().by(label).by(out().values('name').fold())
g.V().group().by(label).by(outE().count())
g.V().group().by(label).by(id)
g.V().group().by(label).by(values('age').max())
g.V().group().by(label).by(values('age').min())
g.V().group().by(label).by(values('age').count())
g.V().groupCount()
g.V().groupCount().by(label)
g.V().groupCount().by('age')
g.V().groupCount().by('name')
g.V().hasLabel('person').groupCount().by(out().count())
g.V().out().groupCount()
g.V().out().groupCount().by('name')
g.V().out().values('name').groupCount()
g.E().groupCount().by(label)
g.E().groupCount().by('weight')
g.V().group().by(label).by(count()).select(keys)
g.V().group().by(label).by(count()).select(values)
g.V().group().by(label).select(keys)
g.V().groupCount().by(label).select(values)
g.V().groupCount().by(label).unfold()
g.V().group().by(label).by('name').unfold()
g.V().project('n','a').by('name').by('age')
g.V().project('n','c').by('name').by(out().count())
g.V().project('n','o').by('name').by(out().values('name').fold())
g.V().hasLabel('person').project('n','a','l').by('name').by('age').by(label)
g.V().project('id','label').by(id).by(label)
g.V().project('a').by(coalesce(values('age'),constant(0)))
g.V().project('a','b').by(constant(1)).by(constant('x'))
g.V().hasLabel('person').project('name','edges').by('name').by(outE().count())
g.E().project('w','o','i').by('weight').by(outV().values('name')).by(inV().values('name'))
g.V(1).project('n').by(out().values('name').fold())
g.V().hasLabel('person').project('n','a').by('name').by('age').select('n')
g.V().hasLabel('person').project('n','a').by('name').by('age').select('a')
g.V().hasLabel('person').project('n','a').by('name').by('age').select('n','a')
g.V().valueMap('name','age').select('name')
g.V().valueMap('name','age').unfold()
g.V().valueMap('name').select(values)
g.V().valueMap('name').select(keys)
g.V().hasLabel('person').valueMap('name','age').select(values).unfold()
g.V().hasLabel('person').elementMap('name').unfold()
g.V().hasLabel('person').fold().unfold().values('name')
g.V().hasLabel('person').fold().count(local)
g.V().fold().count(local)
g.V().values('age').fold().unfold().sum()
g.V().hasLabel('person').has('age',gt(28)).count()
g.V().count().project('c').by()
g.V().hasLabel('person').count().as('c').select('c')
g.V().hasLabel('person').values('age').as('a').select('a')
g.V().hasLabel('person').as('a').values('age').as('b').select('a','b')
g.V().hasLabel('person').as('a').values('age').as('b').select('a','b').by('name').by()
g.V().hasLabel('person').as('a').values('age').as('b').select('b','a').by().by('name')
g.V().hasLabel('person').as('a').out().as('a').select(first,'a')
g.V().hasLabel('person').as('a').out().as('a').select(last,'a')
g.V().hasLabel('person').as('a').out().as('a').select(all,'a')
g.V().hasLabel('person').as('a').out().as('a').select(mixed,'a')
g.V().hasLabel('person').as('a').out().as('b').select(last,'a','b')
g.V().as('a').out().as('a').select('a').by('name')""".split("\n"))

# ---------------------------------------------------------------------------------------------------------------- paths / repeat / branching
add("path", """g.V().path()
g.V(1).out().path()
g.V(1).out().path().by('name')
g.V(1).outE().inV().path().by('name').by('weight').by('name')
g.V(1).outE().inV().path().by(label)
g.V(1).out().out().path().by('name')
g.V().out().path().count(local)
g.V().out().out().path().count(local)
g.V(1).out().as('a').out().as('b').path()
g.V(1).as('x').out().as('y').path()
g.V(1).out().simplePath().path()
g.V(1).out().in().simplePath().path()
g.V(1).out().in().cyclicPath().path()
g.V(1).both().both().cyclicPath().path()
g.V().both().both().simplePath().count()
g.V(1).repeat(out()).times(2).path()
g.V(1).repeat(out()).times(1).path()
g.V(1).repeat(out()).times(3).path()
g.V().repeat(out()).times(2).values('name')
g.V().repeat(out()).times(2).path().by('name')
g.V().repeat(out()).until(has('name','ripple')).path().by('name')
g.V().until(has('name','ripple')).repeat(out()).path().by('name')
g.V(1).repeat(out()).until(has('lang')).values('name')
g.V(1).repeat(out()).until(outE().count().is(0)).values('name')
g.V(1).repeat(out()).until(loops().is(2)).values('name')
g.V(1).repeat(out()).until(loops().is(1)).path()
g.V(1).repeat(out()).emit().path()
g.V(1).repeat(out()).emit().times(2).path()
g.V(1).repeat(out()).times(2).emit().path()
g.V(1).emit().repeat(out()).times(2).path()
g.V(1).emit(has('lang')).repeat(out()).times(2).values('name')
g.V(1).repeat(out()).times(2).emit(has('lang')).values('name')
g.V(1).repeat(out()).emit(has('lang')).until(loops().is(2)).values('name')
g.V(1).repeat(out()).emit().until(has('name','ripple')).values('name')
g.V(1).repeat(out()).until(has('name','ripple')).emit().values('name')
g.V().repeat(both()).times(2).count()
g.V(1).repeat(both()).times(3).dedup().count()
g.V(1).repeat(both().simplePath()).times(2).path()
g.V(1).repeat(both().simplePath()).until(has('name','ripple')).path().by('name')
g.V(1).repeat(out('knows')).times(1).values('name')
g.V(1).repeat(out('knows')).times(0).values('name')
g.V(1).times(2).repeat(out()).values('name')
g.V(3).repeat(__.in()).times(2).values('name')
g.V(3).repeat(__.in()).emit().values('name')
g.V().hasLabel('person').repeat(out('created')).times(1).values('name')
g.V(1).repeat(out().as('x')).times(2).select('x').by('name')
g.V(1).repeat(out()).times(2).tree()
g.V(1).out().out().tree().by('name')
g.V(1).out().tree()
g.V(1).outE().inV().tree().by('name').by(label)
g.V().hasLabel('person').out().tree().by('name')
g.V(1).repeat(out()).until(loops().is(2)).emit().loops()
g.V(1).repeat(out()).emit().loops()
g.V(1).coalesce(out('knows'),out('created')).values('name')
g.V(2).coalesce(out('knows'),out('created')).values('name')
g.V().coalesce(out('knows'),out('created')).values('name')
g.V().coalesce(values('age'),values('lang'))
g.V().coalesce(has('age'),constant('none'))
g.V().coalesce(out('nothing'),constant(0))
g.V(1).union(out('knows'),out('created')).values('name')
g.V(1).union(out(),in()).values('name')
g.V(3).union(in('created'),out()).values('name')
g.V().union(out(),in()).count()
g.V().union(values('name'),values('age'))
g.V(1).union(out().count(),outE().count())
g.V().hasLabel('person').union(values('age'),values('name')).fold()
g.V().choose(has('age'),values('age'),constant('none'))
g.V().choose(has('age'),values('name'))
g.V().choose(hasLabel('person'),out('created'),in('created')).values('name')
g.V().choose(label()).option('person',values('name')).option('software',values('lang'))
g.V().choose(label()).option('person',values('name')).option(none,values('lang'))
g.V().choose(values('age')).option(29,constant('twentynine')).option(none,constant('other'))
g.V().choose(out().count()).option(0,constant('leaf')).option(1,constant('one')).option(none,constant('many'))
g.V().choose(has('age',gt(30)),constant('old'),constant('young'))
g.V().choose(values('age').is(gt(30)),constant('old'),constant('young'))
g.V().hasLabel('person').choose(values('age').is(gt(30)),constant('old'),constant('young'))
g.V().optional(out('knows')).values('name')
g.V().optional(out('nothing')).values('name')
g.V(1).optional(out('knows').out('created')).values('name')
g.V().local(out().count())
g.V().local(out().limit(1))
g.V().local(outE().limit(1)).inV().values('name')
g.V().local(out().values('name').fold())
g.V().local(values('age').fold())
g.V().local(out().order().by('name').limit(1)).values('name')
g.V().map(values('name'))
g.V().map(out().count())
g.V().map(constant('x'))
g.V().flatMap(out())
g.V().flatMap(out().values('name'))
g.V().flatMap(values('name','age'))
g.V().constant(1)
g.V().constant('x').count()
g.V().identity()
g.V().identity().count()
g.V().hasLabel('person').identity().values('name')
g.V().sideEffect(out()).count()
g.V().sideEffect(has('age')).values('name')
g.V().store('x').cap('x')
g.V().store('x').values('name').cap('x')
g.V().aggregate('x').cap('x')
g.V().aggregate('x').by('name').cap('x')
g.V().hasLabel('person').aggregate('x').by('age').cap('x')
g.V().store('x').by('name').cap('x')
g.V().out().aggregate('x').cap('x')
g.V().out().store('x').cap('x')
g.V().hasLabel('person').aggregate('p').out('created').where(within('p')).count()
g.V().hasLabel('person').aggregate('p').out('knows').where(without('p'))
g.V().out('created').aggregate('a').in('created').where(without('a')).values('name')
g.V().groupCount('a').by(label).cap('a')
g.V().group('a').by(label).by('name').cap('a')
g.V().groupCount('a').by(label).groupCount('b').by('name').cap('a','b')
g.V().hasLabel('person').store('a').fold().count(local)
g.V().cap('x')
g.V().aggregate('x').cap('x').unfold().count()
g.V().hasLabel('person').as('a').out('created').as('b').select('a','b').by('name').by('name')
g.V().as('a').out().as('b').where('a',neq('b')).count()
g.V().as('a').out('created').as('b').where('a',eq('b')).count()
g.V().as('a').out().as('b').where(as('a').out().as('b')).count()
g.V().as('a').out('created').in('created').as('b').where('a',neq('b')).select('a','b').by('name')
g.V().as('a').out('created').in('created').as('b').where(neq('a')).select('a','b').by('name')
g.V().as('a').out('created').in('created').as('b').where(neq('a')).values('name')
g.V().where(neq('a'))
g.V().as('a').out().where(eq('a')).count()
g.V(1).out().as('a').where(__.in('created').count().is(gt(1))).values('name')
g.V().where(__.as('a').out('created').count().is(gt(1)))
g.V().hasLabel('person').where(out('knows').where(values('age').is(gt(30)))).values('name')
g.V().hasLabel('person').where(not(out('knows'))).values('name')
g.V().hasLabel('person').where(out('created').and().out('knows')).values('name')
g.V().hasLabel('person').as('p').where(out('created').has('name','lop')).select('p').values('name')
g.V().match(as('a').out().as('b'))""".split("\n"))

# ---------------------------------------------------------------------------------------------------------------- side effects, sack, misc
add("misc", """g.withSideEffect('a',[1,2,3]).V().count()
g.withSideEffect('a',[1,2,3]).inject(1).cap('a')
g.withSideEffect('x',5).V().constant(1).count()
g.withSack(1).V().sack()
g.withSack(1).V().out().sack()
g.withSack(1).V().sack(sum).by(constant(1)).sack()
g.withSack(0).V().outE().sack(sum).by('weight').sack()
g.withSack(1).V(1).outE().sack(mult).by('weight').inV().sack()
g.withSack(1.0d).V(1).repeat(outE().sack(mult).by('weight').inV()).times(2).sack()
g.withSack(0).V().hasLabel('person').sack(sum).by('age').sack()
g.withSack(5).V().sack(minus).by(constant(1)).sack()
g.withSack(1).V().sack(max).by(constant(3)).sack()
g.withSack(1).V().sack(min).by(constant(3)).sack()
g.withSack(1).V().sack(assign).by(constant(3)).sack()
g.V().hasLabel('person').barrier().count()
g.V().hasLabel('person').barrier().values('name')
g.V().hasLabel('person').barrier(2).values('name')
g.V().hasLabel('person').timeLimit(10000).count()
g.V().index()
g.V().hasLabel('person').values('name').index()
g.V().hasLabel('person').local(values('name').fold())
g.V().hasLabel('person').values('name').asString()
g.V().hasLabel('person').values('age').asString()
g.V().hasLabel('person').values('name').toUpper()
g.V().hasLabel('person').values('name').toLower()
g.V().hasLabel('person').values('name').length()
g.V().hasLabel('person').values('name').concat('_x')
g.V().hasLabel('person').values('name').replace('a','A')
g.V().hasLabel('person').values('name').substring(1)
g.V().hasLabel('person').values('name').substring(1,3)
g.V().hasLabel('person').values('name').split('a')
g.V().hasLabel('person').values('name').reverse()
g.inject(' x ').trim()
g.inject(' x ').lTrim()
g.inject(' x ').rTrim()
g.V().hasLabel('person').values('name').fold().asString(local)
g.inject(3,1,2).order().fold()
g.inject(3,1,2).max()
g.inject(3,1,2).min()
g.inject(3,1,2).sum()
g.inject(3,1,2).mean()
g.inject(3,1,2).count()
g.inject(3,1,2).fold()
g.inject(3.0d,1,2L).sum()
g.inject(1,2,3).is(2)
g.inject(1,2,3).is(gt(1))
g.inject(1,2,3,3).dedup()
g.inject([1,2],[3,4]).unfold()
g.inject([1,2],[3,4]).count(local)
g.inject([1,2],[3,4]).sum(local)
g.inject([1,2],[3,4]).max(local)
g.inject([3,1,2]).order(local)
g.inject([3,1,2]).order(local).by(desc)
g.inject([3,1,2]).limit(local,2)
g.inject('a','b').constant('c')
g.inject(1,2,3).map(constant('x'))
g.inject(1,2,3).union(identity(),identity())
g.inject(1,2,3).coalesce(is(2),constant(0))
g.inject(1,2,3).choose(is(2),constant('two'),constant('other'))
g.inject(1,2,3).where(is(gt(1)))
g.inject(1,2,3).not(is(2))
g.inject(1,2,3).fold().unfold()
g.inject(null)
g.inject(null,1).count()
g.inject(true,false)
g.inject(1.5d,2.5d).sum()
g.inject(1.5f,2.5f).sum()
g.inject(1L,2L).sum()
g.inject(2147483647).sum()
g.inject(2147483647,1).sum()
g.inject(1,2.0d).sum()
g.inject(1,2).mean()
g.inject(7,2).math('_ / 2')
g.inject(1,2,3).project('v').by()
g.inject(1,2,3).group().by(is(gt(1)))
g.inject(1,2,3).groupCount().by(is(gt(1)))
g.inject(1,1,2).groupCount()
g.inject('a','b','a').groupCount()
g.inject('a','b','a').dedup().count()
g.inject('a','b','a').fold()
g.inject(1,2,3).sack()
g.inject(1,2,3).path()
g.inject(1,2,3).as('a').select('a')
g.inject(1,2,3).as('a').map(constant(9)).select('a')
g.inject([a:1]).select('a')
g.inject(1).unfold()
g.inject(1).fold()
g.inject().count()""".split("\n"), bc=True)

# ---------------------------------------------------------------------------------------------------------------- classic graph
add("classic", """g.V()
g.V().count()
g.V().label()
g.V().values('name')
g.V().out().values('name')
g.E().values('weight')
g.E().values('weight').sum()
g.E().elementMap()
g.V(1).outE().valueMap()
g.V(1).outE('knows').valueMap(true)
g.V().hasLabel('vertex').count()
g.V().has('age',gt(28)).values('name')
g.V().group().by(label).by(count())
g.V().valueMap(true)
g.E().hasLabel('created').has('weight',gt(0.3f)).count()
g.E().has('weight',0.5f)
g.E().has('weight',gt(0.5d))
g.V(1).repeat(out()).times(2).path().by('name')
g.V().hasLabel('vertex').out('created').dedup().count()""".split("\n"), graph="classic")

# ---------------------------------------------------------------------------------------------------------------- language / errors / script-only
add("lang", """1+1
1+1L
1.5d+1
1.5f+1
1L+1
7/2
7%3
2**3
-5
3*4-2
(1+2)*3
'a'+'b'
'a'*3
[1,2,3]
[1,2,3].size()
[1,2,3].sum()
[3,1,2].sort()
[1,2,3].reverse()
[1,[2,3]]
[a:1,b:2]
[a:1,b:[2,3]]
[:]
[]
[1,2,3].collect{it*2}
[1,2,3].findAll{it>1}
[1,2,3].contains(2)
[1,2,3][1]
[a:1,b:2].a
[a:1,b:2]['b']
[a:1].size()
(1..3)
(3..1)
(1..3).collect{it*it}
true
false
null
!true
1==1
1==2
1!=2
1<2
2>=2
'abc'.length()
'abc'.toUpperCase()
'abc'.substring(1)
'abc'.contains('b')
'a,b'.split(',')
"x${1+1}y"
x=5;x+1
x=5;y=x*2;y
def f(a,b){a+b};f(1,2)
x=[1,2,3];x.add(4);x
x=[1,2,3];x.collect{it+1}
x=1;if(x==1){'one'}else{'other'}
x=0;for(i in 1..3){x=x+i};x
x=0;while(x<3){x++};x
x=[a:1];x.b=2;x
Math.abs(-1)
Math.max(1,2)
Math.sqrt(16)
Math.pow(2,3)
Integer.parseInt('12')
UUID.fromString('11111111-1111-1111-1111-111111111111')
new Date(0)
T.id
T.label
Order.desc
Scope.local
Cardinality.list
Direction.OUT
P.gt(1)
P.within(1,2)
P.gt(1).and(P.lt(3))
TextP.containing('a')
g.V().count().next()
g.V().count().next()+1
g.V().toList()
g.V().limit(2).toList()
g.V().hasLabel('person').values('name').toList()
g.V().hasLabel('person').values('name').toSet()
g.V().values('age').toList().sum()
g.V().values('age').toList().size()
g.V().hasLabel('person').count().toList()
g.V().hasLabel('person').next()
g.V().hasLabel('person').next(2)
g.V().hasLabel('person').hasNext()
g.V().hasLabel('nothing').hasNext()
g.V().iterate()
g.V().hasLabel('person').iterate()
g.V().hasLabel('nothing').next()
g.V().values('age').toList()[0]
g.V().values('name').next()
g.V(1).next().id()
g.V(1).next().label()
g.V(1).next().value('name')
g.E(7).next().id()
g.V().toList().size()
t=g.V().hasLabel('person');t.count()
t=g.V().hasLabel('person').values('name');t.toList()
a=g.V().count().next();b=g.E().count().next();a+b
g.V().count().next()+g.E().count().next()
g.V(1).out().toList().collect{it.value('name')}
g.V().toList().collect{it.id()}
g.V().values('age').toList().collect{it+1}
g.V().hasLabel('person').map{it.get().value('name')}
g.V().hasLabel('person').filter{it.get().value('age')>30}.values('name')
g.V().hasLabel('person').flatMap{it.get().vertices(OUT)}
g.V().hasLabel('person').values('age').map{it.get()*2}
g.V().hasLabel('person').values('age').filter{it.get()>28}
g.V().hasLabel('person').values('name').sideEffect{}.count()
g.V().hasLabel('person').order().by{it.value('age')}.values('name')
g.V().hasLabel('person').group().by{it.value('age')>28}.by('name')
g.V().hasLabel('person').values('age').fold(0){a,b->a+b}
graph
g
graph.traversal().V().count()
g.tx().commit()
graph.tx().commit()
""".split("\n"), bc=False)
# script-only cases whose bytecode form is meaningless were listed above with bc=False; a few also as bytecode:
add("errors", """g.V().foo()
g.V().has()
g.V().out(1)
g.V().values(1)
g.V().hasLabel()
g.V().limit('a')
g.V().order().by('name',foo)
g.V(1).addE('x')
g.V(1).addE('x').to(99)
g.V().select('a')
g.V().repeat(out())
g.V().times(2)
g.V().until(has('a'))
g.V().union()
g.V().coalesce()
g.V().choose()
g.V().property('a')
g.V().valueMap(1,2)
g.V().sum()
g.V().values('name').sum()
g.V().values('name').mean()
g.V().values('age').math('foo')
g.V().values('name').math('_ + 1')
g.V().out().out().out().out().count()
1/0
g.V().hasLabel('person').next(0)
g.E(7).valueMap().foo
x
g.V().has('age',gt('a'))
g.V().has('age',gt(null))
g.V().has(null,1)
g.V(null)
g.V('x')
g.E('x')
g.V().values('name').order().by(desc).limit(-1)
g.V().range(3,1)
g.V().tail(-1)
g.V().where()
g.V().not()
g.V().and()
g.V().or()
g.V().as()
g.V().group().by().by().by()
g.V().project()
g.V().project('a')
g.V().project('a').by('name').by('age')
g.V().path().by()
g.V().sack(sum)
g.V().sack(foo)
g.V().tree().by(1)
g.V().addV()
g.V().addV('x').addV('y')
g.V().drop().V()
""".split("\n"), bc=False)
# the same shapes sent as bytecode (only those the python DSL can express)
add("bcerr", """g.V().foo()
g.V().has()
g.V().select('a')
g.V().union()
g.V().order().by('name',desc)
g.V().values(1)
g.V(1).addE('x')
g.V().project('a')
g.V().valueMap(1,2)
g.V().values('name').sum()
g.V().tail(-1)
g.V().sack(sum)""".split("\n"))

# ---------------------------------------------------------------------------------------------------------------- mutation (each case resets the graph)
MUT = []


def mut(group, steps, **kw):
    n = _counters.get(group, 0) + 1
    _counters[group] = n
    CASES.append(Case(f"{group}_{n:03d}", steps, graph="empty", mutating=True, bc=False, **kw))


mut("mut", ["g.addV('person').property(T.id,1L).property('name','a').next()", "g.V().valueMap(true)"])
mut("mut", ["g.addV('person').property(T.id,1L).property('name','a').property('name','b').next()", "g.V(1).valueMap()"])
mut("mut", ["g.addV('person').property(T.id,1L).property(list,'name','a').property(list,'name','b').next()", "g.V(1).valueMap()"])
mut("mut", ["g.addV('person').property(T.id,1L).property(set,'name','a').property(set,'name','a').property(set,'name','b').next()", "g.V(1).values('name')"])
mut("mut", ["g.addV('person').property(T.id,1L).property(single,'name','a').property(single,'name','b').next()", "g.V(1).values('name')"])
mut("mut", ["g.addV('person').property(T.id,1L).property('name','a').next()", "g.V(1).property('name','b').values('name')"])
mut("mut", ["g.addV('person').property(T.id,1L).property('name','a').next()", "g.V(1).property(list,'name','b').values('name')"])
mut("mut", ["g.addV('person').property(T.id,1L).property('name','a').next()", "g.V(1).property('age',30).valueMap()"])
mut("mut", ["g.addV('person').property(T.id,1L).property('name','a').next()", "g.V(1).property('age',30).property('age',31).values('age')"])
mut("mut", ["g.addV('person').property(T.id,1L).next()", "g.V(1).properties()"])
mut("mut", ["g.addV('person').property(T.id,1L).property('name','a').next()", "g.V(1).properties('name').property('since',2020).valueMap()"])
mut("mut", ["g.addV('person').property(T.id,1L).property('name','a','since',2020).next()", "g.V(1).properties('name').valueMap()"])
mut("mut", ["g.addV('person').property(T.id,1L).property('name','a','since',2020).next()", "g.V(1).properties('name').properties()"])
mut("mut", ["g.addV('a').property(T.id,1L).next()", "g.addV('b').property(T.id,2L).next()", "g.V(1).addE('x').to(__.V(2)).property(T.id,3L).property('w',1).next()", "g.E().valueMap(true)"])
mut("mut", ["g.addV('a').property(T.id,1L).next()", "g.addV('b').property(T.id,2L).next()", "g.V(1).addE('x').to(__.V(2)).property(T.id,3L).next()", "g.E(3).outV().id()"])
mut("mut", ["g.addV('a').property(T.id,1L).next()", "g.addV('b').property(T.id,2L).next()", "g.addE('x').from(__.V(1)).to(__.V(2)).property(T.id,3L).next()", "g.V(1).out().id()"])
mut("mut", ["g.addV('a').property(T.id,1L).as('x').addV('b').property(T.id,2L).as('y').addE('r').from('x').to('y').property(T.id,3L).next()", "g.V(1).outE().inV().id()"])
mut("mut", ["g.addV('a').property(T.id,1L).next()", "g.V(1).addE('self').to(__.V(1)).property(T.id,3L).next()", "g.V(1).both().id()"])
mut("mut", ["g.addV('a').property(T.id,1L).next()", "g.V(1).addE('self').to(__.V(1)).property(T.id,3L).next()", "g.V(1).bothE().id()"])
mut("mut", ["g.addV('a').property(T.id,1L).next()", "g.V(1).addE('self').to(__.V(1)).property(T.id,3L).next()", "g.V(1).in().id()"])
mut("mut", ["g.addV('a').property(T.id,1L).next()", "g.addV('b').property(T.id,2L).next()", "g.V(1).addE('x').to(__.V(2)).property(T.id,3L).next()", "g.E(3).property('w',5).valueMap()"])
mut("mut", ["g.addV('a').property(T.id,1L).next()", "g.addV('b').property(T.id,2L).next()", "g.V(1).addE('x').to(__.V(2)).property(T.id,3L).property('w',5).next()", "g.E(3).properties().drop()", "g.E(3).valueMap()"])
mut("mut", ["g.addV('a').property(T.id,1L).property('n',1).next()", "g.V(1).properties('n').drop()", "g.V(1).valueMap()"])
mut("mut", ["g.addV('a').property(T.id,1L).next()", "g.addV('b').property(T.id,2L).next()", "g.V(1).addE('x').to(__.V(2)).property(T.id,3L).next()", "g.V(1).drop()", "g.V().count()"])
mut("mut", ["g.addV('a').property(T.id,1L).next()", "g.addV('b').property(T.id,2L).next()", "g.V(1).addE('x').to(__.V(2)).property(T.id,3L).next()", "g.V(1).drop()", "g.E().count()"])
mut("mut", ["g.addV('a').property(T.id,1L).next()", "g.addV('b').property(T.id,2L).next()", "g.V(1).addE('x').to(__.V(2)).property(T.id,3L).next()", "g.V(2).drop()", "g.E().count()"])
mut("mut", ["g.addV('a').property(T.id,1L).next()", "g.addV('b').property(T.id,2L).next()", "g.V(1).addE('x').to(__.V(2)).property(T.id,3L).next()", "g.E(3).drop()", "g.V().count()"])
mut("mut", ["g.addV('a').property(T.id,1L).next()", "g.addV('a').property(T.id,2L).next()", "g.V().drop()", "g.V().count()"])
mut("mut", ["g.addV('a').property(T.id,1L).next()", "g.addV('a').property(T.id,1L).next()"])
mut("mut", ["g.addV('a').property(T.id,1L).next()", "g.addV('a').property(T.id,2L).next()", "g.V(1).addE('x').to(__.V(2)).property(T.id,3L).next()", "g.V(1).addE('x').to(__.V(2)).property(T.id,3L).next()"])
mut("mut", ["g.addV('a').property(T.id,1L).next()", "g.V(1).addE('x').to(__.V(9)).next()"])
mut("mut", ["g.addV('a').property(T.id,1L).next()", "g.V().hasLabel('a').count()"])
mut("mut", ["g.addV().property(T.id,1L).next()", "g.V(1).label()"])
mut("mut", ["g.addV('a').property(T.id,1L).property('x',1).next()", "g.V(1).property('x',null).valueMap()"])
mut("mut", ["g.addV('a').property(T.id,1L).next()", "g.V().property('k','v').valueMap()"])
mut("mut", ["g.addV('a').property(T.id,1L).next()", "g.addV('a').property(T.id,2L).next()", "g.V().property('k','v').count()"])
mut("mut", ["g.addV('a').property(T.id,1L).next()", "g.addV('a').property(T.id,2L).next()", "g.V().property('k',__.id()).valueMap()"])
mut("mut", ["g.addV('a').property(T.id,1L).property('n',5).next()", "g.V(1).property('n',__.values('n').math('_ + 1')).values('n')"])
mut("mut", ["g.addV('a').property(T.id,1L).next()", "g.V(1).property('a',1,'b',2).properties('a').valueMap()"])
mut("mut", ["g.addV('a').property(T.id,1L).property('i',1).property('l',2L).property('d',1.5d).property('f',2.5f).property('s','x').property('b',true).next()", "g.V(1).valueMap()"])
mut("mut", ["g.addV('a').property(T.id,1L).property('u',UUID.fromString('11111111-1111-1111-1111-111111111111')).next()", "g.V(1).values('u')"])
mut("mut", ["g.addV('a').property(T.id,1L).property('l',[1,2,3]).next()", "g.V(1).values('l')"])
mut("mut", ["g.addV('a').property(T.id,1L).property('d',new Date(0)).next()", "g.V(1).values('d')"])
mut("mut", ["g.mergeV([(T.id):1L,(T.label):'a',name:'x']).next()", "g.V().valueMap(true)"])
mut("mut", ["g.mergeV([(T.id):1L,(T.label):'a']).next()", "g.mergeV([(T.id):1L,(T.label):'a']).option(Merge.onMatch,[name:'y']).next()", "g.V().valueMap(true)"])
mut("mut", ["g.addV('a').property(T.id,1L).next()", "g.addV('a').property(T.id,2L).next()", "g.mergeE([(T.label):'x',(Direction.OUT):1L,(Direction.IN):2L]).next()", "g.E().count()"])
mut("mut", ["g.addV('a').property(T.id,1L).next()", "g.addV('a').property(T.id,2L).next()", "g.mergeE([(T.label):'x',(Direction.OUT):1L,(Direction.IN):2L]).next()", "g.mergeE([(T.label):'x',(Direction.OUT):1L,(Direction.IN):2L]).next()", "g.E().count()"])
mut("mut", ["g.inject(1,2,3).addV('n').property('v',__.identity()).count()", "g.V().values('v').order()"])
mut("mut", ["g.addV('a').property(T.id,1L).next()", "g.V(1).as('a').addE('l').to('a').next()", "g.E().count()"])
mut("mut", ["g.addV('a').property(T.id,1L).next()", "g.V(1).addV('b').property(T.id,2L).count()", "g.V().count()"])
mut("mut", ["g.addV('a').property(T.id,1L).next()", "g.addV('b').property(T.id,2L).next()", "g.V(1).as('a').V(2).addE('to').from('a').property(T.id,3L).next()", "g.V(2).in().id()"])

# batching (chunked results: 200 / 206 / 204)
for k, (script, batch) in enumerate([("g.V()", 2), ("g.V()", 1), ("g.V()", 6), ("g.V()", 5), ("g.V().hasLabel('none')", 2), ("g.V().id()", 4), ("g.E().id()", 3),
                                     ("g.inject(1,2,3,4,5,6,7)", 3), ("g.V().values('name')", 100)]):
    CASES.append(Case(f"batch_{k + 1:03d}", script, batch=batch, bc=True, ordered=True))


# ---------------------------------------------------------------------------------------------------------------- random traversals
def random_scripts(n, seed):
    rnd = random.Random(seed)
    labels = ["knows", "created"]
    vprops = ["name", "age", "lang"]

    def lab():
        r = rnd.random()
        if r < .4:
            return ""
        if r < .8:
            return "'%s'" % rnd.choice(labels)
        return "'knows','created'"

    def vpred():
        c = rnd.choice(["has('age')", "has('name')", "has('lang')", "hasLabel('person')", "hasLabel('software')",
                        "has('age',gt(%d))" % rnd.choice([26, 28, 30, 33]), "has('age',lt(%d))" % rnd.choice([28, 30, 34]),
                        "has('name',within('marko','josh','lop'))", "has('name',startingWith('%s'))" % rnd.choice("mvljrp"),
                        "where(out().count().is(gt(%d)))" % rnd.choice([0, 1]), "has('name',neq('marko'))", "hasId(%d)" % rnd.randint(1, 6),
                        "has(id,within(%d,%d))" % (rnd.randint(1, 6), rnd.randint(1, 6)), "not(has('age'))", "where(out('created'))",
                        "where(__.in('knows'))", "or(has('age'),has('lang'))", "and(has('age'),hasLabel('person'))"])
        return c

    def epred():
        return rnd.choice(["has('weight',gt(%s))" % rnd.choice(["0.3d", "0.5d", "0.9d"]), "hasLabel('knows')", "hasLabel('created')",
                           "has('weight',lte(0.5d))", "has('weight',within(0.4d,1.0d))", "hasId(%d)" % rnd.randint(7, 12)])

    def sub_v():
        return rnd.choice(["out()", "__.in()", "both()", "out('knows')", "out('created')", "__.in('created')", "outE().inV()", "both('knows')"])

    def vstep(depth):
        r = rnd.random()
        if r < .25:
            return "%s(%s)" % (rnd.choice(["out", "in", "both"]), lab()), "V"
        if r < .35:
            return "%s(%s)" % (rnd.choice(["outE", "inE", "bothE"]), lab()), "E"
        if r < .5:
            return vpred(), "V"
        if r < .58:
            return "dedup()", "V"
        if r < .66:
            return "repeat(%s).times(%d)" % (rnd.choice(["out()", "both()", "out('knows')", "__.in()"]), rnd.randint(1, 3)), "V"
        if r < .72:
            return "union(%s,%s)" % (sub_v(), sub_v()), "V"
        if r < .78:
            return "coalesce(%s,%s)" % (sub_v(), sub_v()), "V"
        if r < .83:
            return "optional(%s)" % sub_v(), "V"
        if r < .88:
            return "as('%s')" % rnd.choice("abc"), "V"
        if r < .91:
            return "simplePath()", "V"
        if r < .93:
            return "where(%s)" % sub_v(), "V"
        return "sideEffect(out())", "V"

    def estep():
        r = rnd.random()
        if r < .3:
            return rnd.choice(["inV()", "outV()", "otherV()", "bothV()"]), "V"
        if r < .55:
            return epred(), "E"
        if r < .7:
            return "dedup()", "E"
        return "as('%s')" % rnd.choice("abc"), "E"

    def vterm():
        return rnd.choice(["count()", "values('name')", "values('age')", "values('name','age')", "valueMap('name')", "valueMap(true)", "elementMap()",
                           "id()", "label()", "path()", "path().by('name')", "groupCount().by(label)", "group().by(label).by('name')",
                           "groupCount().by('name')", "project('n','c').by('name').by(out().count())", "tree()", "select('a')", "properties('name')",
                           "order().by(id).fold()", "order().by('name').values('name')", "values('name').dedup().order().fold()",
                           "order().by(id).limit(%d)" % rnd.randint(1, 4), "order().by(id).range(1,%d)" % rnd.randint(2, 5),
                           "order().by(id).tail(%d)" % rnd.randint(1, 3), "fold().count(local)", "outE().count()", "both().dedup().count()",
                           "values('age').sum()", "values('age').max()", "values('age').min()", "values('age').mean()", "hasLabel('person').values('age').fold()",
                           "select('a','b')", "select('a').by('name')", "as('a').out().as('b').select('a','b').by('name')", "sack()", "loops()",
                           "coalesce(values('age'),constant(-1))", "choose(has('age'),values('age'),constant(0))", "map(values('name'))",
                           "flatMap(out().values('name'))", "local(out().count())", "local(outE().limit(1))"])

    def eterm():
        return rnd.choice(["count()", "values('weight')", "valueMap()", "elementMap()", "id()", "label()", "path()", "groupCount().by(label)",
                           "values('weight').sum()", "values('weight').max()", "order().by('weight').by(id).values('weight')", "order().by(id).limit(2)",
                           "properties()", "project('w','l').by('weight').by(label)", "group().by(label).by('weight')", "valueMap(true)",
                           "outV().values('name')", "inV().values('name')", "tree()"])

    out = []
    for _ in range(n):
        start = rnd.choice(["g.V()", "g.V()", "g.V()", "g.V(%d)" % rnd.randint(1, 6), "g.V(%d,%d)" % (rnd.randint(1, 6), rnd.randint(1, 6)),
                            "g.V().hasLabel('person')", "g.V().hasLabel('software')", "g.E()", "g.E(%d)" % rnd.randint(7, 12)])
        ctx = "E" if start.startswith("g.E") else "V"
        parts = [start]
        for _ in range(rnd.randint(0, 4)):
            if ctx == "V":
                s, ctx = vstep(0)
            else:
                s, ctx = estep()
            parts.append(s)
        parts.append(vterm() if ctx == "V" else eterm())
        out.append(".".join(parts))
    return out


RANDOM_N = 700
RANDOM_SEED = 20260926
for _i, _s in enumerate(random_scripts(RANDOM_N, RANDOM_SEED)):
    CASES.append(Case(f"rnd_{_i + 1:04d}", _s))
