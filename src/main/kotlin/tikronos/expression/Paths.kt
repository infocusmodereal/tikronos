package tikronos.expression

import tikronos.invalid
import tikronos.model.Item
import tikronos.model.Value

internal sealed interface Step {
    data class Name(val name: String) : Step

    data class Index(val index: Int) : Step
}

internal data class Path(val root: String, val steps: List<Step> = emptyList()) {
    fun read(item: Item): Value? =
        steps.fold(item[root]) { value, step ->
            when (step) {
                is Step.Name -> value?.takeIf { it.type == "M" }?.members()?.get(step.name)
                is Step.Index -> value?.takeIf { it.type == "L" }?.entries()?.getOrNull(step.index)
            }
        }

    fun overlaps(other: Path) =
        root == other.root && steps.zip(other.steps).all { (a, b) -> a == b }
}

internal fun validatePaths(paths: List<Path>) {
    for ((index, path) in paths.withIndex()) {
        for (prior in paths.take(index).filter { it.root == path.root }) {
            if (path.overlaps(prior)) invalid("Document paths overlap")
            val divergence = path.steps.zip(prior.steps).first { (a, b) -> a != b }
            if (divergence.first::class != divergence.second::class)
                invalid("Document paths conflict")
        }
    }
}

internal fun project(item: Item, paths: List<Path>?): Item {
    if (paths == null) return item
    return paths
        .groupBy(Path::root)
        .mapNotNull { (name, group) ->
            item[name]?.let { selected(it, group.map(Path::steps)) }?.let { name to it }
        }
        .toMap()
}

private fun selected(value: Value, paths: List<List<Step>>): Value? {
    if (paths.any(List<Step>::isEmpty)) return value
    val children = paths.groupBy({ it.first() }, { it.drop(1) })
    return when (value.type) {
        "M" ->
            value
                .members()
                .mapNotNull { (name, child) ->
                    children[Step.Name(name)]?.let { selected(child, it) }?.let { name to it }
                }
                .toMap()
                .takeIf { it.isNotEmpty() }
                ?.let(Value::map)
        "L" ->
            value
                .entries()
                .mapIndexedNotNull { index, child ->
                    children[Step.Index(index)]?.let { selected(child, it) }
                }
                .takeIf { it.isNotEmpty() }
                ?.let(Value::list)
        else -> null
    }
}

/** Apply siblings against original list indices so deletes cannot shift a later action's target. */
internal fun patch(item: Item, changes: Map<Path, Value?>): Item =
    patchValue(
            Value.map(item),
            changes.map { (path, value) -> (listOf(Step.Name(path.root)) + path.steps) to value },
        )
        .members()

private fun patchValue(parent: Value, changes: List<Pair<List<Step>, Value?>>): Value {
    val grouped = changes.groupBy({ it.first.first() }, { it.first.drop(1) to it.second })
    fun changed(old: Value?, edits: List<Pair<List<Step>, Value?>>): Value? {
        if (edits.first().first.isEmpty()) return edits.single().second
        return patchValue(old ?: invalid("Missing parent document path"), edits)
    }
    return when (parent.type) {
        "M" -> {
            val result = parent.members().toMutableMap()
            grouped.forEach { (step, edits) ->
                val name =
                    (step as? Step.Name)?.name ?: invalid("Document path requires a map field")
                val next = changed(result[name], edits)
                if (next == null) result.remove(name) else result[name] = next
            }
            Value.map(result)
        }
        "L" -> {
            val list = parent.entries()
            val edits =
                grouped.mapKeys { (step, _) ->
                    (step as? Step.Index)?.index ?: invalid("Document path requires a list index")
                }
            val values =
                list.mapIndexedNotNull { i, old ->
                    if (i in edits) changed(old, edits.getValue(i)) else old
                }
            Value.list(
                values +
                    edits
                        .filterKeys { it >= list.size }
                        .toSortedMap()
                        .values
                        .mapNotNull { changed(null, it) }
            )
        }
        else -> invalid("Invalid parent document path")
    }
}
