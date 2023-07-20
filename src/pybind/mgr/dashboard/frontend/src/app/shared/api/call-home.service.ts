import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';
import { MgrModuleService } from './mgr-module.service';
import { HttpClient } from '@angular/common/http';

@Injectable({
  providedIn: 'root'
})
export class CallHomeService {
  baseURL = 'api/call_home';

  constructor(private http: HttpClient, private mgrModuleService: MgrModuleService) {}

  list(params: any) {
    return this.http.get<any>(`${this.baseURL}/${params.ibmId}/
    ${params.companyName}/${params.firstName}/${params.lastName}/${params.email}`);
  }

  set(params: any) {
    return this.http.put<any>(
      `${this.baseURL}/${params.tenantId}/${params.ibmId}/${params.companyName}/
    ${params.firstName}/${params.lastName}/${params.email}`,
      {}
    );
  }

  getCallHomeConfig(): Observable<string> {
    return this.mgrModuleService
      .getConfig('dashboard')
      .pipe(map((config: any) => config.CALL_HOME_REMIND_LATER_ON));
  }

  getCallHomeStatus(): Observable<boolean> {
    return this.mgrModuleService.list().pipe(
      map((moduleData: any) => {
        const callHomeModule = moduleData.find((module: any) => module.name === 'call_home_agent');
        return callHomeModule ? callHomeModule.enabled : false;
      })
    );
  }

  downloadReport(type: string) {
    return this.http.get(`${this.baseURL}/download/${type}`);
  }

  info() {
    return this.http.get(`${this.baseURL}/info`);
  }
}
