import { Component, EventEmitter, OnInit, Output } from '@angular/core';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { MgrModuleService } from '../../api/mgr-module.service';
import { NotificationService } from '../../services/notification.service';
import { NotificationType } from '../../enum/notification-type.enum';
import { BlockUI, NgBlockUI } from 'ng-block-ui';
import { timer } from 'rxjs';
import { CallHomeNotificationService } from '../../services/call-home-notification.service';
import { CdFormGroup } from '../../forms/cd-form-group';
import { FormControl, Validators } from '@angular/forms';
import { CallHomeService } from '../../api/call-home.service';
import { CdForm } from '../../forms/cd-form';
import { TextToDownloadService } from '../../services/text-to-download.service';

@Component({
  selector: 'cd-call-home-modal',
  templateUrl: './call-home-modal.component.html',
  styleUrls: ['./call-home-modal.component.scss']
})
export class CallHomeModalComponent extends CdForm implements OnInit {
  @BlockUI()
  blockUI: NgBlockUI;

  @Output() callHomeEnabled = new EventEmitter<boolean>(); // Change the type as needed

  callHomeForm: CdFormGroup;
  isConfigured = false;
  title = $localize`Configure IBM Call Home`;

  report: any;


  constructor(
    public activeModal: NgbActiveModal,
    private mgrModuleService: MgrModuleService,
    private notificationService: NotificationService,
    private callHomeNotificationService: CallHomeNotificationService,
    private callHomeSerive: CallHomeService,
    private textToDownloadService: TextToDownloadService,
  ) {
    super();
  }

  ngOnInit() {
    this.createForm();
    this.callHomeSerive.getCallHomeStatus().subscribe((data: boolean) => {
      this.isConfigured = data;
      if (data) {
        this.title = $localize`Download Reports`;
      }
      this.loadingReady();
    })
  }

  createForm() {
    this.callHomeForm = new CdFormGroup({
      customerNumber: new FormControl(null, [Validators.required]),
      firstName: new FormControl(null, [Validators.required]),
      lastName: new FormControl(null, [Validators.required]),
      email: new FormControl(null, [Validators.required, Validators.email]),
      phone: new FormControl(null, [Validators.required]),
      address: new FormControl(null),
      companyName: new FormControl(null),
      countryCode: new FormControl(null, [Validators.required]),
      licenseAgrmt: new FormControl(false, [Validators.requiredTrue])
    });
  }

  download(type: string) {
    const fileName = `${type}_${new Date().toLocaleDateString()}`
    this.callHomeSerive.downloadReport(type).subscribe((data: any) => {
      this.report = data;
      this.textToDownloadService.download(
        JSON.stringify(this.report,null,2),
        `${fileName}.json`
      );
    })
  }

  stop() {
    this.callHome(false);
  }

  submit() {
    const customerNumber = this.callHomeForm.getValue('customerNumber');
    const firstName = this.callHomeForm.getValue('firstName');
    const lastName = this.callHomeForm.getValue('lastName');
    const email = this.callHomeForm.getValue('email');
    const phone = this.callHomeForm.getValue('phone');
    const address = this.callHomeForm.getValue('address');
    const companyName = this.callHomeForm.getValue('companyName');
    const countryCode = this.callHomeForm.getValue('countryCode');

    this.mgrModuleService.updateConfig('call_home_agent', {
      icn: customerNumber,
      customer_first_name: firstName,
      customer_last_name: lastName,
      customer_email: email,
      customer_phone: phone,
      customer_address: address,
      customer_company_name: companyName,
      customer_country_code: countryCode
    }).subscribe({
      error: () => this.callHomeForm.setErrors({ cdSubmitButton: true }),
      complete: () => this.callHome()
    });
  }

  callHome(enable = true) {
    const fnWaitUntilReconnected = () => {
      timer(2000).subscribe(() => {
        // Trigger an API request to check if the connection is
        // re-established.
        this.mgrModuleService.list().subscribe(
          () => {
            // Resume showing the notification toasties.
            this.notificationService.suspendToasties(false);
            // Unblock the whole UI.
            this.blockUI.stop();
            // Reload the data table content.
            if (enable) {
              this.notificationService.show(NotificationType.success, $localize`Activated IBM Call Home Agent`);
              this.callHomeNotificationService.setVisibility(false);
              this.activeModal.close();
            } else {
              this.notificationService.show(NotificationType.success, $localize`Deactivated IBM Call Home Agent`);
              this.activeModal.close();
            }
          },
          () => {
            fnWaitUntilReconnected();
          }
        );
      });
    };

    if(enable) {
      this.mgrModuleService.enable('call_home_agent').subscribe(
        () => undefined,
        () => {
          // Suspend showing the notification toasties.
          this.notificationService.suspendToasties(true);
          // Block the whole UI to prevent user interactions until
          // the connection to the backend is reestablished
          this.blockUI.start($localize`Reconnecting, please wait ...`);
          fnWaitUntilReconnected();
        }
      );
    } else {

      this.mgrModuleService.disable('call_home_agent').subscribe(
        () => undefined,
        () => {
          // Suspend showing the notification toasties.
          this.notificationService.suspendToasties(true);
          // Block the whole UI to prevent user interactions until
          // the connection to the backend is reestablished
          this.blockUI.start($localize`Reconnecting, please wait ...`);
          fnWaitUntilReconnected();
        }
      );
    }
  }
}
