import { Transform } from 'stream';

export class MyTransform extends Transform {
  constructor(options?: any) {
    super(options);
    // Listen to the 'end' event on the readable side of the stream
    this.on('finish', () => {
      // console.log('Reading is done');
    });
  }

  _transform(chunk: any, encoding: string, callback: Function) {
    try {
      this.sleepAndPush(chunk, callback);
    } catch (err) {
      callback(err);
    }
  }

  private pushAndSleep(chunk: any, callback: Function) {
    this.push(chunk);

    setTimeout(() => {
      callback();
    }, 1000);
  }

  private sleepAndPush(chunk: any, callback: Function) {
    setTimeout(() => {
      this.push(chunk);
      callback();
    }, 1000);
  }
}
