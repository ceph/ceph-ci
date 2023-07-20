import { Component, OnInit } from '@angular/core';
import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { ModalService } from '../../services/modal.service';
import { MgrModuleService } from '../../api/mgr-module.service';
import { Observable } from 'rxjs';
import { environment } from '~/environments/environment';
import { NotificationService } from '../../services/notification.service';
import { NotificationType } from '../../enum/notification-type.enum';
import { CallHomeNotificationService } from '../../services/call-home-notification.service';
import { CallHomeModalComponent } from '../call-home-modal/call-home-modal.component';

@Component({
  selector: 'cd-call-home-notification',
  templateUrl: './call-home-notification.component.html',
  styleUrls: ['./call-home-notification.component.scss']
})
export class CallHomeNotificationComponent implements OnInit {
  mgrModuleConfig$: Observable<object>;

  displayNotification = false;
  notificationSeverity = 'warning';
  environment = environment;

  modalRef: NgbModalRef;

  remindAfterDays = 90;

  constructor(
    private modalService: ModalService,
    private mgrModuleService: MgrModuleService,
    private notificationService: NotificationService,
    private callHomeNotificationService: CallHomeNotificationService
  ) {}

  ngOnInit(): void {
    this.callHomeNotificationService.update.subscribe((visible) => {
      this.displayNotification = visible;
    });
  }

  openModal(): void {
    this.modalRef = this.modalService.show(
      CallHomeModalComponent,
      {
        submitAction: () => {
          this.modalRef.close();
        }
      },
      { size: 'lg' }
    );
  }

  onDismissed(): void {
    this.callHomeNotificationService.hide();
    const dateNow = new Date();
    const remindOn = new Date(
      dateNow.getTime() + 1000 * 60 * 60 * (this.remindAfterDays * 24)
    ).toDateString();
    this.mgrModuleService
      .updateConfig('dashboard', { CALL_HOME_REMIND_LATER_ON: remindOn })
      .subscribe(() => {
        this.notificationService.show(
          NotificationType.success,
          $localize`Call Home activation reminder muted`,
          $localize`You have muted the Call Home activation for ${this.remindAfterDays} days.`
        );
      });
  }
}
