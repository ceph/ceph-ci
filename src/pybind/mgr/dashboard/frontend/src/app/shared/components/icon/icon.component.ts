import {
  Component,
  Input,
  OnChanges,
  OnInit,
  ViewEncapsulation,
  SimpleChanges
} from '@angular/core';
import { ICON_TYPE, IconSize } from '../../enum/icons.enum';

@Component({
  selector: 'cd-icon',
  templateUrl: './icon.component.html',
  styleUrl: './icon.component.scss',
  encapsulation: ViewEncapsulation.None
})
export class IconComponent implements OnInit, OnChanges {
  @Input() type!: keyof typeof ICON_TYPE;
  @Input() size: IconSize = IconSize.size16;
  @Input() customClass: string = '';
  // No CSS class will be applied.
  @Input() useDefault: boolean = false;

  icon: string;

  ngOnInit() {
    this.updateIcon();
  }

  ngOnChanges(changes: SimpleChanges) {
    if (changes['type']) {
      this.updateIcon();
    }
  }

  private updateIcon() {
    this.icon = ICON_TYPE[this.type];
  }
}
