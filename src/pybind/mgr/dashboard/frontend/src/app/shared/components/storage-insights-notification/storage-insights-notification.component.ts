import { Component, OnInit } from '@angular/core';
import { NgbModalRef } from '@ng-bootstrap/ng-bootstrap';
import { ModalService } from '../../services/modal.service';
import { MgrModuleService } from '../../api/mgr-module.service';
import { Observable } from 'rxjs';
import { environment } from '~/environments/environment';
import { NotificationService } from '../../services/notification.service';
import { NotificationType } from '../../enum/notification-type.enum';
import { StorageInsightsModalComponent } from '../storage-insights-modal/storage-insights-modal.component';
import { StorageInsightsNotificationService } from '../../services/storage-insights-notification.service';
import { CallHomeNotificationService } from '../../services/call-home-notification.service';

@Component({
  selector: 'cd-storage-insights-notification',
  templateUrl: './storage-insights-notification.component.html',
  styleUrls: ['./storage-insights-notification.component.scss']
})
export class StorageInsightsNotificationComponent implements OnInit {
  mgrModuleConfig$: Observable<object>;

  displayNotification = false;
  callHomeEnabled: boolean = false;
  notificationSeverity = 'warning';
  environment = environment;

  modalRef: NgbModalRef;

  remindAfterDays = 90;

  constructor(
    private modalService: ModalService,
    private mgrModuleService: MgrModuleService,
    private notificationService: NotificationService,
    private storageInsightsNotificationService: StorageInsightsNotificationService,
    private CallHomeNotificationService: CallHomeNotificationService
  ) {}

  ngOnInit(): void {
    this.storageInsightsNotificationService.update.subscribe((visible) => {
      this.displayNotification = visible;
    });
    this.CallHomeNotificationService.update.subscribe((enabled: boolean) => {
      this.callHomeEnabled = !enabled;
    });
  }

  openModal(): void {
    this.modalRef = this.modalService.show(
      StorageInsightsModalComponent,
      {
        submitAction: () => {
          this.modalRef.close();
        }
      },
      { size: 'lg' }
    );
  }

  onDismissed(): void {
    this.storageInsightsNotificationService.hide();
    const dateNow = new Date();
    const remindOn = new Date(
      dateNow.getTime() + 1000 * 60 * 60 * (this.remindAfterDays * 24)
    ).toDateString();
    this.mgrModuleService
      .updateConfig('dashboard', { STORAGE_INSIGHTS_REMIND_LATER_ON: remindOn })
      .subscribe(() => {
        this.notificationService.show(
          NotificationType.success,
          $localize`Storage Insights activation reminder muted`,
          $localize`You have muted the Storage Insights activation for ${this.remindAfterDays} days.`
        );
      });
  }
}
