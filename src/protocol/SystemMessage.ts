export enum SystemMessageType {
  QueryRequest = 1,
  QueryResponse = 2,
  QueryPropagate = 3,

} export class SystemMessage {

  public readonly messageType: SystemMessageType;

  constructor(
    messageType: SystemMessageType,
  ) {
    this.messageType = messageType;
  }

  static deserialize(
    msg: any,
  ): SystemMessage {
    return msg as SystemMessage;
  }
}
