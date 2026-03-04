import { Component, Input, OnInit } from '@angular/core';
import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { ModalCdsService } from '../../services/modal-cds.service';
import { MgrModuleService } from '../../api/mgr-module.service';
import { Observable } from 'rxjs';
import { environment } from '~/environments/environment';
import { NotificationService } from '../../services/notification.service';
import { NotificationType } from '../../enum/notification-type.enum';
import { CallHomeNotificationService } from '../../services/call-home-notification.service';
import { CallHomeModalComponent } from '../call-home-modal/call-home-modal.component';
import { CallHomeService } from '../../api/call-home.service';

@Component({
  selector: 'cd-call-home-notification',
  templateUrl: './call-home-notification.component.html',
  styleUrls: ['./call-home-notification.component.scss']
})
export class CallHomeNotificationComponent implements OnInit {
  @Input() callHomeEnabled: boolean | null = null;
  @Input() callHomeEnabledWarning: boolean = false;
  mgrModuleConfig$: Observable<object>;

  displayNotification = false;
  notificationSeverity = 'warning';
  environment = environment;

  modalRef: NgbModalRef;

  remindAfterDays = 90;

  constructor(
    private cdsModalService: ModalCdsService,
    private mgrModuleService: MgrModuleService,
    private notificationService: NotificationService,
    private callHomeNotificationService: CallHomeNotificationService,
    private callHomeService: CallHomeService
  ) {}

  ngOnInit(): void {
    this.callHomeNotificationService.update.subscribe((visible) => {
      this.displayNotification = visible;
    });
  }

  openModal(): void {
    this.modalRef = this.cdsModalService.show(
      CallHomeModalComponent,
      {
        submitAction: () => {
          this.modalRef.close();
        }
      }
    );
  }

  onDismissedActivate(): void {
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

  confirmAutoEnabled(): void {
    this.callHomeService.confirmAutoEnabled().subscribe({
      next: () => {
        this.callHomeNotificationService.hide();
        this.notificationService.show(
          NotificationType.success,
          $localize`Call Home acknowledged`
        );
      },
      error: (err) => {
        this.notificationService.show(
          NotificationType.error,
          $localize`Error confirming Call Home`,
          err?.error?.detail || err?.message
        );
      }
    });
  }
}
