# CS460 Project 1 - Relational Operators and Execution Models

by Gabriel Jiménez

---

This README showcases some implementation details.
To see the project instructions, check out the [handout](./handout.pdf).


## Use of `LazyList` within the `next` method

I make extensive use of `LazyList`, to implement the `next` method of volcano `Operator`s mainly because I like the fact that it makes
writing incorrect code so difficult and the fact that it's much easier to conceptualize than a `while` loop (at least for me).
However, I'm aware it might reduce code clarity.

```scala
val iterator = LazyList continually input.next() takeWhile (_.isDefined) map (_.get)
```

This allows me to easily manipulate the entirety of the remaining tuples in `input`
as if I had them all in a list, but without actually retrieving the tuples.
Calling `headOption` on the resulting `LazyList` allows me then to retrieve the first
qualifying tuple.

#### Joins

This approach is even more convenient for implementing the join operators `Join`, `LateJoin`, and `Stitch`.
Indeed, for each tuple in the `left` input, I need to produce a list of tuples, instead of a single
tuple.
With the `LazyList` approach, I simply use the `map` operator on the above
`iterator`, and I construct, for each tuple, its respective "buffer of matching tuples".
This produces a `LazyList` of buffers that are not generated until it is actually
necessary.

The algorithm is simple:

- Keep an output buffer to emit tuples.

- If the output buffer is empty, get the next buffer of matching tuples (using `headOption`)
and use it to fill the output buffer.

- While the output buffer is not empty, emit a tuple from the buffer.

The join algorithm I implemented here is a hash join,
so I keep a `HashMap` containing the entirety of right input. Since not every
key found in the left input will match some key from the right input,
it's possible that when I attempt to get all the matching tuples from some
left tuple, my `HashMap` will return `None`.

So, instead of producing a `LazyList` of buffers, I produce a `LazyList` of optional
buffers.
To get the list of buffers I want, I use the `collect` method on the `LazyList`
to discard the `None`s and get the value from the `Some`s.
In this case, a `collect` would be equivalent to a `filter (_.isDefined) map (_.get)`.



