import { Component, OnInit } from '@angular/core';
import { CdFormGroup } from '../../forms/cd-form-group';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { NotificationService } from '../../services/notification.service';
import { NotificationType } from '../../enum/notification-type.enum';
import { FormControl, Validators } from '@angular/forms';
import { ActionLabelsI18n } from '../../constants/app.constants';
import { CallHomeService } from '../../api/call-home.service';
import { StorageInsightsNotificationService } from '../../services/storage-insights-notification.service';
import { StorageInsightsService } from '../../api/storage-insights.service';
import { CdForm } from '../../forms/cd-form';

@Component({
  selector: 'cd-storage-insights-modal',
  templateUrl: './storage-insights-modal.component.html',
  styleUrls: ['./storage-insights-modal.component.scss']
})
export class StorageInsightsModalComponent extends CdForm implements OnInit {
  modalForm: CdFormGroup;
  tenantForm: CdFormGroup;

  title = $localize`Configure IBM Storage Insights`;
  action = $localize`Opt in`;

  tenantsList: any;
  selectedTenant: any[] = [];

  tenantUrl: string;
  tenantId: string;
  tenantCompanyName: string;

  isConfigured = false;
  callHomeEnabled = false;

  constructor(
    public activeModal: NgbActiveModal,
    public actionLabels: ActionLabelsI18n,
    private notificationService: NotificationService,
    private callHomeService: CallHomeService,
    private storageInsightsNotificationService: StorageInsightsNotificationService,
    private storageInsightsService: StorageInsightsService
  ) {
    super();
  }

  ngOnInit(): void {
    this.createForm();
    this.callHomeService.getCallHomeStatus().subscribe((enabled) => {
      this.callHomeEnabled = enabled;

      if (!enabled) this.modalForm.disable();
    });
    this.storageInsightsService.getStorageInsightsStatus().subscribe((status: boolean) => {
      this.isConfigured = status;

      status ? this.populateForm() : this.loadingReady();
    });
  }

  createForm() {
    this.modalForm = new CdFormGroup({
      ibmId: new FormControl({ value: null, disabled: this.callHomeEnabled }, [
        Validators.required
      ]),
      companyName: new FormControl({ value: null, disabled: this.callHomeEnabled }, [
        Validators.required
      ]),
      firstName: new FormControl({ value: null, disabled: this.callHomeEnabled }, [
        Validators.required
      ]),
      lastName: new FormControl({ value: null, disabled: this.callHomeEnabled }, [
        Validators.required
      ]),
      email: new FormControl({ value: null, disabled: this.callHomeEnabled }, [Validators.required])
    });
  }

  populateForm() {
    this.action = $localize`Update`;
    this.callHomeService.info().subscribe((data: any) => {
      this.modalForm.get('ibmId').setValue(data.IBM_storage_insights.owner_ibm_id);
      this.modalForm.get('companyName').setValue(data.IBM_storage_insights.owner_company_name);
      this.modalForm.get('firstName').setValue(data.IBM_storage_insights.owner_first_name);
      this.modalForm.get('lastName').setValue(data.IBM_storage_insights.owner_last_name);
      this.modalForm.get('email').setValue(data.IBM_storage_insights.owner_email);
      const tenantId = data.IBM_storage_insights.owner_IBM_tenant_id;
      this.modalForm.get('tenants').setValue(tenantId);
      this.fetchTenants(this.modalForm.value, tenantId);
    });
    this.loadingReady();
  }

  onSelectChange(): void {
    this.tenantCompanyName = this.selectedTenant['company-name'];
    this.tenantUrl = this.selectedTenant['external_url'];
    let urlSplit = this.tenantUrl.split('/');
    this.tenantId = urlSplit[urlSplit.length - 1];
  }

  fetchTenants(formFields: any, tenantId = ''): void {
    this.callHomeService.list(formFields).subscribe(
      (data: any) => {
        this.tenantsList = data['si-instances'];
        if (tenantId) {
          this.selectedTenant = this.tenantsList.filter((tenant: any) =>
            tenant['external_url'].includes(tenantId)
          );
        } else {
          this.selectedTenant = this.tenantsList[0];
        }
        this.onSelectChange();
      },
      (error) => {
        this.modalForm.setErrors({ cdSubmitButton: true });
        this.notificationService.show(
          NotificationType.error,
          $localize`Failed to fetch tenants`,
          error.error.detail
        );
      }
    );
  }

  optIn(formFields: any) {
    const notificationMessage = this.isConfigured
      ? $localize`Updated IBM Storage Insights Configuration`
      : $localize`Activated IBM Storage Insights`;
    formFields['tenantId'] = this.tenantId;
    this.callHomeService.set(formFields).subscribe({
      error: () => this.modalForm.setErrors({ cdSubmitButton: true }),
      complete: () => {
        this.storageInsightsNotificationService.setVisibility(false);
        this.notificationService.show(NotificationType.success, notificationMessage);
        this.storageInsightsNotificationService.setVisibility(false);
        this.activeModal.close();
      }
    });
  }
}
