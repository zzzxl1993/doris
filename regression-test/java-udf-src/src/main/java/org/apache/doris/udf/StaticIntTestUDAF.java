// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.udf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class StaticIntTestUDAF {
    static {
        System.out.println("static load should only print once StaticIntTestUDAF");
    }
    private static int value = 0;
    public static class State {
        public long counter = 0;
    }

    public State create() {
        return new State();
    }

    public void destroy(State state) {
    }

    public void reset(State state) {
        state.counter = 0;
    }

    public void add(State state, Integer val) {
        if (val == null) return;
        state.counter += val;
    }

    public void serialize(State state, DataOutputStream out) throws IOException {
        out.writeLong(state.counter);
    }

    public void deserialize(State state, DataInputStream in) throws IOException {
        state.counter = in.readLong();
    }

    public void merge(State state, State rhs) {
        state.counter += rhs.counter;
    }

    public long getValue(State state) {
        value = value + 1;
        System.out.println("getValue StaticIntTestUDAF " + value + " " + state.counter);
        return state.counter + value;
    }
}
