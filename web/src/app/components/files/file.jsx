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

import React, { Component } from 'react';
import classnames from 'classnames';
import { Uploader, Downloader } from 'app/services/fileTransfer';
import withProgress from './withProgress';

export class File extends Component {

  onRemove = () => {
    this.props.onRemove(this.props.id);
  }

  savedToDisk = false;

  componentDidUpdate() {
    const { isCompleted, response } = this.props.status;
    if (isCompleted && !this.props.isUpload) {
      this.saveToDisk(response)
    }
  }

  saveToDisk({ fileName, blob }) {
    if (this.savedToDisk) {
      return;
    }

    this.savedToDisk = true;

    const a = document.createElement("a");
    a.href = window.URL.createObjectURL(blob);
    a.download = fileName;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
  }

  render() {
    let {
      isFailed,
      isProcessing,
      isCompleted,
      progress,
      error
    } = this.props.status;

    let name = this.props.name;

    const className = classnames(
      "grv-file-transfer-file-list-item",
      isFailed && "--failed",
      isProcessing && "--processing",
      isCompleted && "--completed",
    )

    return (
      <div className={className}>
        <div className="grv-file-transfer-file-path">
          {name}
          {isFailed && <div> {error} </div> }
        </div>
        <div className="grv-file-transfer-file-status">
          {isFailed &&
            <div>
              failed
            </div>
          }
          {isProcessing &&
            <div>
              {progress}%
            </div>
          }
          {isCompleted &&
            <div>
              completed
            </div>
          }
        </div>
        <div className="grv-file-transfer-file-close">
          <a onClick={this.onRemove}>
            <i className="fa fa-times" aria-hidden="true"></i>
          </a>
        </div>
      </div>
    )
  }
}

export const FileToSend = withProgress(Uploader)(File);
export const FileToReceive = withProgress(Downloader)(File);