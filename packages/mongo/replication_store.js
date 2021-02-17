/**
 * @summary Constructor for a Collection
 * @locus Anywhere
 * @instancename collection
 * @class
 * @param {String} name The name of the collection.  If null, creates an unmanaged (unsynchronized) local collection.
 * @param {Object} [options]
 * @param {Object} options.connection The server connection that will manage this collection. Uses the default connection if not specified.  Pass the return value of calling [`DDP.connect`](#ddp_connect) to specify a different server. Pass `null` to specify no connection. Unmanaged (`name` is null) collections cannot specify a connection.
 * @param {String} options.idGeneration The method of generating the `_id` fields of new documents in this collection.  Possible values:

 - **`'STRING'`**: random strings
 - **`'MONGO'`**:  random [`Mongo.ObjectID`](#mongo_object_id) values

 The default id generation technique is `'STRING'`.
 * @param {Function} options.transform An optional transformation function. Documents will be passed through this function before being returned from `fetch` or `findOne`, and before being passed to callbacks of `observe`, `map`, `forEach`, `allow`, and `deny`. Transforms are *not* applied for the callbacks of `observeChanges` or to cursors returned from publish functions.
 * @param {Boolean} options.defineMutationMethods Set to `false` to skip setting up the mutation methods that enable insert/update/remove from client code. Default `true`.
 */
export class ReplicationStore {
  constructor(connector) {
    this.connector = connector;
    this.collection = connector._collection;
  }

  // Called at the beginning of a batch of updates. batchSize is the number
  // of update calls to expect.
  //
  // XXX This interface is pretty janky. reset probably ought to go back to
  // being its own function, and callers shouldn't have to calculate
  // batchSize. The optimization of not calling pause/remove should be
  // delayed until later: the first call to update() should buffer its
  // message, and then we can either directly apply it at endUpdate time if
  // it was the only update, or do pauseObservers/apply/apply at the next
  // update() if there's another one.
  beginUpdate(batchSize, reset) {
    // pause observers so users don't see flicker when updating several
    // objects at once (including the post-reconnect reset-and-reapply
    // stage), and so that a re-sorting of a query can take advantage of the
    // full _diffQuery moved calculation instead of applying change one at a
    // time.
    if (batchSize > 1 || reset)
      this.collection.pauseObservers();

    if (reset)
      this.collection.remove({});
  }

  // Apply an update.
  // XXX better specify this interface (not in terms of a wire message)?
  update(msg) {
    const mongoId = MongoID.idParse(msg.id);
    const doc = this.collection._docs.get(mongoId);

    switch(msg.msg) {
      case 'replace':
        this._processReplace(mongoId, doc, msg);
        break;
      case 'added':
        if (doc) {
          throw new Error('Expected not to find a document already present for an add');
        }

        this._processAdded(mongoId, msg);
        break;
      case 'removed':
        if (!doc) {
          throw new Error('Expected to find a document already present for removed');
        }

        this._processRemoved(mongoId, doc, msg);
        break;
      case 'changed':
        if (!doc) {
          throw new Error('Expected to find a document to change');
        }

        this._processChanged(mongoId, doc, msg);
        break;
      default:
        throw new Error(`I don't know how to deal with this message: ${msg.msg}`);
    }
  }

  _processReplace(mongoId, doc, msg) {
    // Is this a "replace the whole doc" message coming from the quiescence
    // of method writes to an object? (Note that 'undefined' is a valid
    // value meaning "remove it".)
    if (!msg.replace) {
      if (doc) {
        this.collection.remove(mongoId);
      }
    } else if (!doc) {
      this.collection.insert(msg.replace);
    } else {
      // XXX check that replace has no $ ops
      this.collection.update(mongoId, msg.replace);
    }
  }

  _processAdded(mongoId, msg) {
    this.collection.insert({ _id: mongoId, ...msg.fields });
  }

  _processRemoved(mongoId, _doc, _msg) {
    this.collection.remove(mongoId);
  }

  _processChanged(mongoId, doc, msg) {
    const keys = Object.keys(msg.fields);
    if (keys.length > 0) {
      const modifier = {};
      keys.forEach(key => {
        const value = msg.fields[key];
        if (EJSON.equals(doc[key], value)) {
          return;
        }
        if (typeof value === 'undefined') {
          if (!modifier.$unset) {
            modifier.$unset = {};
          }
          modifier.$unset[key] = 1;
        } else {
          if (!modifier.$set) {
            modifier.$set = {};
          }
          modifier.$set[key] = value;
        }
      });
      if (Object.keys(modifier).length > 0) {
        this.collection.update(mongoId, modifier);
      }
    }
  }

  // Called at the end of a batch of updates.
  endUpdate() {
    this.collection.resumeObservers();
  }

  // Called around method stub invocations to capture the original versions
  // of modified documents.
  saveOriginals() {
    this.collection.saveOriginals();
  }

  retrieveOriginals() {
    return this.collection.retrieveOriginals();
  }

  // Used to preserve current versions of documents across a store reset.
  getDoc(id) {
    return this.connector.findOne(id);
  }
}
