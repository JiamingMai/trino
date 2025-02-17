/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hive.metastore.glue.v1;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static java.util.Objects.requireNonNull;

public final class Memoizers
{
    private Memoizers() {}

    public static <T> UnaryOperator<T> memoizeLast()
    {
        return new Simple<>();
    }

    public static <I, O> Function<I, O> memoizeLast(Function<I, O> transform)
    {
        return new Transforming<>(transform);
    }

    public static <T, U, R> BiFunction<T, U, R> memoizeLast(BiFunction<T, U, R> transform)
    {
        requireNonNull(transform, "transform is null");
        Function<Pair<T, U>, R> memoized = memoizeLast(pair -> transform.apply(pair.first, pair.second));
        return (a, b) -> memoized.apply(new Pair<>(a, b));
    }

    private static final class Simple<T>
            implements UnaryOperator<T>
    {
        private T lastInput;

        @Override
        public T apply(T input)
        {
            if (!Objects.equals(lastInput, input)) {
                lastInput = input;
            }
            return lastInput;
        }
    }

    private static final class Transforming<I, O>
            implements Function<I, O>
    {
        private final Function<I, O> transform;
        private I lastInput;
        private O lastOutput;
        private boolean inputSeen;

        private Transforming(Function<I, O> transform)
        {
            this.transform = requireNonNull(transform, "transform is null");
        }

        @Override
        public O apply(I input)
        {
            if (!inputSeen || !Objects.equals(lastInput, input)) {
                lastOutput = transform.apply(input);
                lastInput = input;
                inputSeen = true;
            }
            return lastOutput;
        }
    }

    private record Pair<T, U>(T first, U second) {}
}
