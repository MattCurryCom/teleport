/*
Copyright 2015 Gravitational, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

import { Store } from 'nuclear-js';
import { Record, Map } from 'immutable';
import * as AT from './actionTypes';

class File extends Record({
  isFailed: false,
  isProcessing: false,
  isCompleted: false,
  progress: "",
  path: "",
  error: ""
}) {
}

class FileTransferStore extends Record({
  isOpen: true,
  files: new Map(),
}){

  constructor(params){
    super(params);
  }

  setOpen(isOpen) {
    return this.set('isOpen', isOpen);
  }

  add(json) {
    return this.setIn(['files', json.name], new File(json));
  }

  remove(id) {
    return this.deleteIn(['files', id ])
  }
}

export default Store({
  getInitialState() {
    return new FileTransferStore();
  },

  initialize() {
    this.on(AT.SET_OPEN, (state, isOpen) => state.setOpen(isOpen));
    this.on(AT.ADD_FILE, (state, json) => state.add(json));
    this.on(AT.REMOVE_FILE, (state, id) => state.remove(id));
  }
})


