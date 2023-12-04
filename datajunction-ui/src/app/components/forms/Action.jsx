export class Action {
  static Add = new Action('add');
  static Edit = new Action('edit');

  constructor(name) {
    this.name = name;
  }
}
