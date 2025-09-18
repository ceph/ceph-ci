import { Component, EventEmitter, OnInit, Output } from '@angular/core';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { MgrModuleService } from '../../api/mgr-module.service';
import { NotificationService } from '../../services/notification.service';
import { NotificationType } from '../../enum/notification-type.enum';
import { BlockUI, NgBlockUI } from 'ng-block-ui';
import { BehaviorSubject, Observable } from 'rxjs';
import { CdFormGroup } from '../../forms/cd-form-group';
import { FormControl, Validators } from '@angular/forms';
import { CallHomeService } from '../../api/call-home.service';
import { CdForm } from '../../forms/cd-form';
import { TextToDownloadService } from '../../services/text-to-download.service';
import { ConnectivityStatus } from '../../models/call-home.model';
import { switchMap } from 'rxjs/operators';
import { Icons } from '../../enum/icons.enum';
import { CallHomeNotificationService } from '../../services/call-home-notification.service';

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
  callHomeStatus$: Observable<ConnectivityStatus>;
  callHomeStatusSubject = new BehaviorSubject<ConnectivityStatus>(null);
  callHomeRefreshLoading = false;

  report: any;

  icons = Icons;

  constructor(
    public activeModal: NgbActiveModal,
    private mgrModuleService: MgrModuleService,
    private notificationService: NotificationService,
    private callHomeSerive: CallHomeService,
    private textToDownloadService: TextToDownloadService,
    private callHomeNotificationService: CallHomeNotificationService
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
    });
    this.callHomeStatus$ = this.callHomeStatusSubject.pipe(
      switchMap(() => this.callHomeSerive.status())
    );
    this.callHomeStatusSubject.next(null);
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
      licenseAgrmt: new FormControl(null, [Validators.required])
    });
  }

  download(type: string) {
    const fileName = `${type}_${new Date().toLocaleDateString()}`;
    this.callHomeSerive.downloadReport(type).subscribe((data: any) => {
      this.report = data;
      this.textToDownloadService.download(JSON.stringify(this.report, null, 2), `${fileName}.json`);
    });
  }

  stop() {
    this.callHome(false);
  }

  submit() {
    if (this.callHomeForm.errors) {
      this.callHomeForm.setErrors({ cdSubmitButton: true });
      return;
    }
    const customerNumber = this.callHomeForm.getValue('customerNumber');
    const firstName = this.callHomeForm.getValue('firstName');
    const lastName = this.callHomeForm.getValue('lastName');
    const email = this.callHomeForm.getValue('email');
    const phone = this.callHomeForm.getValue('phone');
    const address = this.callHomeForm.getValue('address');
    const companyName = this.callHomeForm.getValue('companyName');
    const countryCode = this.callHomeForm.getValue('countryCode');

    this.mgrModuleService
      .updateConfig('call_home_agent', {
        icn: customerNumber,
        customer_first_name: firstName,
        customer_last_name: lastName,
        customer_email: email,
        customer_phone: phone,
        customer_address: address,
        customer_company_name: companyName,
        customer_country_code: countryCode
      })
      .subscribe({
        next: () => this.callHome(),
        error: () => this.callHomeForm.setErrors({ cdSubmitButton: true }),
        complete: () => this.callHomeNotificationService.setVisibility(false)
      });
  }

  callHome(enable = true) {
    this.mgrModuleService.updateModuleState(
      'call_home_agent',
      !enable,
      null,
      '',
      enable ? $localize`Activated IBM Call Home Agent`
       : $localize`Deactivated IBM Call Home Agent`,
      false,
      $localize`Enabling Call Home Module...`,
      this.activeModal
    );

    if (!enable) this.callHomeNotificationService.setVisibility(true);
  }

  testConnectivity() {
    this.callHomeRefreshLoading = true;
    this.callHomeSerive.testConnectivity().subscribe({
      complete: () => {
        this.notificationService.show(
          NotificationType.success,
          $localize`Refreshed call home connectivity status.`
        );
        this.callHomeStatusSubject.next(null);
        this.callHomeRefreshLoading = false;
      }
    });
  }
}
