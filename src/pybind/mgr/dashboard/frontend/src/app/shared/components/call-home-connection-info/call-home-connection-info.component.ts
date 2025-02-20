import { Component, Input } from '@angular/core';

@Component({
  selector: 'cd-call-home-connection-info',
  templateUrl: './call-home-connection-info.component.html',
  styleUrl: './call-home-connection-info.component.scss'
})
export class CallHomeConnectionInfoComponent {
  @Input() message: string;
}
