import { Component, Input } from '@angular/core';
import { Icons } from '~/app/shared/enum/icons.enum';
import { ConnectivityStatus } from '~/app/shared/models/call-home.model';

@Component({
  selector: 'cd-call-home-connectivity-status',
  templateUrl: './call-home-connectivity-status.component.html',
  styleUrl: './call-home-connectivity-status.component.scss'
})
export class CallHomeConnectivityStatusComponent {
  @Input() status: ConnectivityStatus;

  icons = Icons;
}
