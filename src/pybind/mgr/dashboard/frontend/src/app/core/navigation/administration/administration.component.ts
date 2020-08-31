import { Component } from '@angular/core';
import { CallHomeModalComponent } from '~/app/shared/components/call-home-modal/call-home-modal.component';
import { StorageInsightsModalComponent } from '~/app/shared/components/storage-insights-modal/storage-insights-modal.component';

import { Permission } from '~/app/shared/models/permissions';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalService } from '~/app/shared/services/modal.service';
import { environment } from '~/environments/environment';

@Component({
  selector: 'cd-administration',
  templateUrl: './administration.component.html',
  styleUrls: ['./administration.component.scss']
})
export class AdministrationComponent {
  userPermission: Permission;
  configOptPermission: Permission;
  environment = environment;

  constructor(private authStorageService: AuthStorageService, private modalService: ModalService) {
    const permissions = this.authStorageService.getPermissions();
    this.userPermission = permissions.user;
    this.configOptPermission = permissions.configOpt;
  }

  openCallHomeModal() {
    this.modalService.show(CallHomeModalComponent, null, { size: 'lg' });
  }

  openSIModal() {
    this.modalService.show(StorageInsightsModalComponent, null, { size: 'lg' });
  }
}
